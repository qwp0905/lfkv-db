use std::{
  fs::{Metadata, OpenOptions},
  ops::{Div, Mul},
  path::PathBuf,
  sync::Arc,
  time::Duration,
};

use crossbeam::queue::ArrayQueue;

use crate::{
  Error, Page, Result, SafeWork, SharedWorkThread, SingleWorkThread, ToArc,
  UnwrappedSender, WorkBuilder,
};

use super::{Pread, Pwrite};

const DEFAULT_READ_THREADS: usize = 1;
const DEFAULT_WRITE_THREADS: usize = 1;

pub struct FinderConfig {
  pub path: PathBuf,
  pub batch_delay: Duration,
  pub batch_size: usize,
  pub read_threads: Option<usize>,
  pub write_threads: Option<usize>,
}

pub struct Finder<const N: usize> {
  read_ths: Arc<SharedWorkThread<usize, std::io::Result<Page<N>>>>,
  write_ths: Arc<SharedWorkThread<(usize, Page<N>), std::io::Result<()>>>,
  batch_th: Arc<SingleWorkThread<(usize, Page<N>), std::io::Result<()>>>,
  flush_th: Arc<SingleWorkThread<(), std::io::Result<()>>>,
  meta_th: Arc<SingleWorkThread<(), std::io::Result<Metadata>>>,
}
impl<const N: usize> Finder<N> {
  pub fn open(config: FinderConfig) -> Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(&config.path)
      .map_err(Error::IO)?;

    let ff = file.try_clone().map_err(Error::IO)?;
    let flush_th = WorkBuilder::new()
      .name(format!("flush {}", config.path.to_string_lossy()))
      .stack_size(N)
      .single()
      .no_timeout(move |_| ff.sync_all())
      .new_arc();

    let read_ths = WorkBuilder::new()
      .name(format!("read {}", config.path.to_string_lossy()))
      .stack_size(N)
      .shared(config.read_threads.unwrap_or(DEFAULT_READ_THREADS))
      .build(|_| {
        let fd = file.try_clone().map_err(Error::IO)?;
        let work = SafeWork::no_timeout(move |index: usize| {
          let mut page = Page::new();
          fd.pread(page.as_mut(), index.mul(N) as u64)?;
          Ok(page)
        });
        Ok(work)
      })?
      .new_arc();

    let write_ths = WorkBuilder::new()
      .name(format!("read {}", config.path.to_string_lossy()))
      .stack_size(N)
      .shared(config.write_threads.unwrap_or(DEFAULT_WRITE_THREADS))
      .build(|_| {
        let fd = file.try_clone().map_err(Error::IO)?;
        let work = SafeWork::no_timeout(move |(index, page): (usize, Page<N>)| {
          fd.pwrite(page.as_ref(), index.mul(N) as u64)?;
          Ok(())
        });
        Ok(work)
      })?
      .new_arc();

    let wc = write_ths.clone();
    let fc = flush_th.clone();
    let wait = ArrayQueue::new(config.batch_size);

    let batch_th = WorkBuilder::new()
      .name(format!("batch {}", config.path.to_string_lossy()))
      .stack_size(N)
      .single()
      .with_timer(config.batch_delay, move |v| {
        if let Some(((index, page), done)) = v {
          match wc.send_await((index, page)) {
            Ok(Err(err)) => {
              done.must_send(Ok(Err(err)));
              return false;
            }
            Err(err) => {
              done.must_send(Err(err));
              return false;
            }
            _ => {}
          };
          let _ = wait.push(done);
          if !wait.is_full() {
            return false;
          }
        }

        match fc.send_await(()) {
          Ok(Err(_)) => return false,
          Err(_) => return false,
          _ => {}
        }

        while let Some(done) = wait.pop() {
          done.must_send(Ok(Ok(())));
        }

        true
      })
      .new_arc();

    let mf = file.try_clone().map_err(Error::IO)?;
    let meta_th = WorkBuilder::new()
      .name(format!("meta {}", config.path.to_string_lossy()))
      .stack_size(N)
      .single()
      .no_timeout(move |_| mf.metadata())
      .new_arc();

    Ok(Self {
      read_ths,
      write_ths,
      batch_th,
      flush_th,
      meta_th,
    })
  }

  pub fn read(&self, index: usize) -> Result<Page<N>> {
    self.read_ths.send_await(index)?.map_err(Error::IO)
  }

  pub fn write(&self, index: usize, page: Page<N>) -> Result {
    self.batch_th.send_await((index, page))?.map_err(Error::IO)
  }

  pub fn fsync(&self) -> Result {
    self.flush_th.send_await(())?.map_err(Error::IO)
  }

  pub fn close(&self) {
    self.batch_th.finalize();
    self.write_ths.close();
    self.read_ths.close();
    self.flush_th.close();
  }

  pub fn len(&self) -> Result<usize> {
    let meta = self.meta_th.send_await(())?.map_err(Error::IO)?;
    Ok((meta.len() as usize).div(N))
  }
}
