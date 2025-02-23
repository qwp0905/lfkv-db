use std::{
  fs::{Metadata, OpenOptions},
  ops::{Add, Mul},
  path::PathBuf,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  time::Duration,
};

use crate::{BackgroundThread, BackgroundWork, Error, Page, Result, UnwrappedSender};

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
  read_ths: Vec<BackgroundThread<usize, std::io::Result<Page<N>>>>,
  read_c: AtomicUsize,
  write_ths: Vec<BackgroundThread<(usize, Page<N>), std::io::Result<()>>>,
  write_c: AtomicUsize,
  flush_th: Arc<BackgroundThread<(), std::io::Result<()>>>,
  meta_th: BackgroundThread<(), std::io::Result<Metadata>>,
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
    let flush_th = Arc::new(BackgroundThread::new(
      format!("flush {}", config.path.to_string_lossy()),
      N,
      BackgroundWork::no_timeout(move |_| ff.fsync()),
    ));

    let mut read_ths = vec![];
    for i in 0..config.read_threads.unwrap_or(DEFAULT_READ_THREADS) {
      let rf = file.copy().map_err(Error::IO)?;
      let th = BackgroundThread::new(
        format!("read {} {}", config.path.to_string_lossy(), i),
        N,
        BackgroundWork::no_timeout(move |index: usize| {
          let mut page = Page::new_empty();
          rf.pread(page.as_mut(), index.mul(N) as u64)?;
          Ok(page)
        }),
      );
      read_ths.push(th);
    }

    let mut write_ths = vec![];
    for i in 0..config.write_threads.unwrap_or(DEFAULT_WRITE_THREADS) {
      let fc = flush_th.clone();
      let wf = file.copy().map_err(Error::IO)?;
      let mut wait = Vec::with_capacity(config.batch_size);
      let th = BackgroundThread::new(
        format!("write {} {}", config.path.to_string_lossy(), i),
        N,
        BackgroundWork::<(usize, Page<N>), std::io::Result<()>>::with_timer(
          config.batch_delay,
          move |v| {
            if let Some(((index, page), done)) = v {
              if let Err(err) = wf.pwrite(page.as_ref(), index.mul(N) as u64) {
                done.must_send(Err(err));
                return false;
              }

              wait.push(done);
              if wait.len().lt(&config.batch_size) {
                return false;
              }
            }

            if let Err(_) = fc.send_await(()) {
              return false;
            }

            wait.drain(..).for_each(|done| done.must_send(Ok(())));
            true
          },
        ),
      );
      write_ths.push(th);
    }

    let mf = file.copy().map_err(Error::IO)?;
    let meta_th = BackgroundThread::new(
      format!("meta {}", config.path.to_string_lossy()),
      1,
      BackgroundWork::no_timeout(move |_| mf.metadata()),
    );

    Ok(Self {
      read_ths,
      read_c: AtomicUsize::new(0),
      write_ths,
      write_c: AtomicUsize::new(0),
      flush_th,
      meta_th,
    })
  }

  pub fn read(&self, index: usize) -> Result<Page<N>> {
    let i = self
      .read_c
      .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
        Some(v.add(1).rem_euclid(self.read_ths.len()))
      })
      .unwrap();
    self.read_ths[i].send_await(index).map_err(Error::IO)
  }

  pub fn write(&self, index: usize, page: Page<N>) -> Result {
    let i = self
      .write_c
      .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
        Some(v.add(1).rem_euclid(self.write_ths.len()))
      })
      .unwrap();
    self.write_ths[i]
      .send_await((index, page))
      .map_err(Error::IO)
  }

  pub fn fsync(&self) -> Result {
    self.flush_th.send_await(()).map_err(Error::IO)
  }

  pub fn close(&self) {
    for th in self.read_ths.iter() {
      th.close();
    }
    for th in self.write_ths.iter() {
      th.close();
    }
    self.flush_th.close();
  }

  pub fn len(&self) -> Result<usize> {
    let meta = self.meta_th.send_await(()).map_err(Error::IO)?;
    Ok(meta.len() as usize / N)
  }
}
