use std::{
  fs::{Metadata, OpenOptions},
  ops::{Div, Mul},
  path::PathBuf,
  sync::Arc,
  time::Duration,
};

use crossbeam::queue::ArrayQueue;

use crate::{
  Error, Page, Result, SafeWork, SafeWorkThread, SharedWorkThread, UnwrappedSender,
};

use super::DirectIO;

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
  batch_th: Arc<SafeWorkThread<(usize, Page<N>), std::io::Result<()>>>,
  flush_th: Arc<SafeWorkThread<(), std::io::Result<()>>>,
  meta_th: SafeWorkThread<(), std::io::Result<Metadata>>,
}
impl<const N: usize> Finder<N> {
  pub fn open(config: FinderConfig) -> Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .direct_io(&config.path)
      .map_err(Error::IO)?;

    let ff = file.copy().map_err(Error::IO)?;
    let flush_th = Arc::new(SafeWorkThread::new(
      format!("flush {}", config.path.to_string_lossy()),
      N,
      SafeWork::no_timeout(move |_| ff.fsync()),
    ));

    let read_ths = SharedWorkThread::build(
      format!("read {}", config.path.to_string_lossy()),
      N,
      config.read_threads.unwrap_or(DEFAULT_READ_THREADS),
      |_| {
        let fd = file.copy().map_err(Error::IO)?;
        let work = SafeWork::no_timeout(move |index: usize| {
          let mut page = Page::new_empty();
          fd.pread(page.as_mut(), index.mul(N) as u64)?;
          Ok(page)
        });
        Ok(work)
      },
    )?;
    let read_ths = Arc::new(read_ths);

    let write_ths = SharedWorkThread::build(
      format!("read {}", config.path.to_string_lossy()),
      N,
      config.write_threads.unwrap_or(DEFAULT_WRITE_THREADS),
      |_| {
        let fd = file.copy().map_err(Error::IO)?;
        let work = SafeWork::no_timeout(move |(index, page): (usize, Page<N>)| {
          fd.pwrite(page.as_ref(), index.mul(N) as u64)?;
          Ok(())
        });
        Ok(work)
      },
    )?;

    let write_ths = Arc::new(write_ths);
    let wc = write_ths.clone();
    let fc = flush_th.clone();
    let wait = ArrayQueue::new(config.batch_size);

    let batch_th = Arc::new(SafeWorkThread::new(
      format!("batch {}", config.path.to_string_lossy()),
      N,
      SafeWork::with_timer(config.batch_delay, move |v| {
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
      }),
    ));

    let mf = file.copy().map_err(Error::IO)?;
    let meta_th = SafeWorkThread::new(
      format!("meta {}", config.path.to_string_lossy()),
      1,
      SafeWork::no_timeout(move |_| mf.metadata()),
    );

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
