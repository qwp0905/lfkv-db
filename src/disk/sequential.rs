use std::{
  collections::HashMap,
  fs::{File, OpenOptions},
  io::{Read, Seek, SeekFrom, Write},
  path::Path,
  sync::{Arc, Mutex, RwLock},
  time::{Duration, Instant},
};

use crate::{
  ContextReceiver, Error, Page, Result, ShortenedMutex, ShortenedRwLock,
  StoppableChannel, ThreadPool,
};

pub struct Sequential<const N: usize> {
  background: ThreadPool<Result<()>>,
  delay: Duration,
  count: usize,
  file: Arc<Mutex<File>>,
  cursor: Arc<RwLock<usize>>,
  io_c: StoppableChannel<Page<N>, usize>,
}
impl<const N: usize> Sequential<N> {
  fn start_io(&self, rx: ContextReceiver<Page<N>, usize>) {
    let count = self.count;
    let delay = self.delay;
    let file = self.file.clone();
    let cursor = self.cursor.clone();

    self.background.schedule(move || {
      let mut start = Instant::now();
      let mut point = delay;
      let mut m = HashMap::new();
      while let Ok(o) = rx.recv_done_or_timeout(delay) {
        if let Some((p, done)) = o {
          let mut c = cursor.wl();
          m.insert(*c, done);
          file.l().write_all(p.as_ref()).map_err(Error::IO)?;
          *c += 1;

          if m.len() < count {
            point -= Instant::now().duration_since(start).min(point);
            start = Instant::now();
            continue;
          }
        }

        if m.len() != 0 {
          file.l().sync_all().map_err(Error::IO)?;
          m.drain().for_each(|(i, done)| done.send(i).unwrap());
        }

        point = delay;
        start = Instant::now();
      }
      Ok(())
    });
  }

  pub fn read(&self, index: usize) -> Result<Page<N>> {
    let mut file = self.file.l();
    let mut page = Page::new();
    file
      .seek(SeekFrom::Start((index * N) as u64))
      .map_err(Error::IO)?;
    file.read_exact(page.as_mut()).map_err(Error::IO)?;
    file
      .seek(SeekFrom::Start((*self.cursor.rl() * N) as u64))
      .map_err(Error::IO)?;
    Ok(page)
  }

  pub fn write(&self, page: Page<N>) -> usize {
    self.io_c.send_with_done(page).recv().unwrap()
  }
}
