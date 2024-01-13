use std::{
  path::Path,
  sync::{Arc, Mutex},
};

use utils::{size, EmptySender, ShortenedMutex};

use crate::{
  disk::{Page, PageSeeker},
  error::{Error, Result},
  thread::{ContextReceiver, StoppableChannel, ThreadPool},
  transaction::LockManager,
};

mod cache;
use cache::*;
mod list;

struct PageBuffer {
  page: Page,
  dirty: bool,
}
impl PageBuffer {
  fn new(page: Page, dirty: bool) -> Self {
    Self { page, dirty }
  }

  fn copy(&self) -> Page {
    self.page.copy()
  }

  fn remove_dirty(&mut self) {
    self.dirty = false
  }
}

pub struct BufferPool {
  cache: Arc<Mutex<Cache<usize, PageBuffer>>>,
  disk: Arc<PageSeeker>,
  background: ThreadPool<Result<()>>,
  locks: Arc<LockManager>,
  flush_c: StoppableChannel<Vec<(usize, Page)>>,
}

impl BufferPool {
  pub fn open<T>(
    path: T,
    cache_size: usize,
    locks: Arc<LockManager>,
  ) -> Result<(Self, StoppableChannel<Vec<(usize, Page)>>)>
  where
    T: AsRef<Path>,
  {
    let disk = Arc::new(PageSeeker::open(path)?);
    let cache = Arc::new(Mutex::new(Cache::new(cache_size)));
    let background = ThreadPool::new(1, size::mb(30), "buffer pool", None);
    let (flush_c, rx) = StoppableChannel::new();
    let buffer_pool =
      Self::new(cache, disk, background, locks, flush_c.clone());
    buffer_pool.start_background(rx);
    Ok((buffer_pool, flush_c))
  }

  fn new(
    cache: Arc<Mutex<Cache<usize, PageBuffer>>>,
    disk: Arc<PageSeeker>,
    background: ThreadPool<Result<()>>,
    locks: Arc<LockManager>,
    flush_c: StoppableChannel<Vec<(usize, Page)>>,
  ) -> Self {
    Self {
      cache,
      disk,
      background,
      locks,
      flush_c,
    }
  }

  fn start_background(&self, rx: ContextReceiver<Vec<(usize, Page)>>) {
    let disk = self.disk.clone();
    let locks = self.locks.clone();
    let cache = self.cache.clone();
    self.background.schedule(move || {
      while let Ok((entries, done_c)) = rx.recv_all() {
        for (index, page) in entries {
          let lock = locks.fetch_write_lock(index);
          disk.write(index, page)?;
          cache.l().get_mut(&index).map(|pb| pb.remove_dirty());
          drop(lock);
        }
        disk.fsync()?;
        done_c.map(|d| d.close());
      }

      return Ok(());
    })
  }
}

impl BufferPool {
  pub fn get(&self, index: usize) -> Result<Page> {
    if let Some(pb) = { self.cache.l().get(&index) } {
      return Ok(pb.copy());
    };

    let page = self.disk.read(index)?;
    if page.is_empty() {
      return Err(Error::NotFound);
    }

    self
      .cache
      .l()
      .insert(index, PageBuffer::new(page.copy(), false));
    return Ok(page);
  }

  pub fn insert(&self, index: usize, page: Page) -> Result<()> {
    if let Some(pb) = { self.cache.l().get_mut(&index) } {
      pb.page = page;
      return Ok(());
    };

    if let Some((i, pb)) =
      self.cache.l().insert(index, PageBuffer::new(page, true))
    {
      if pb.dirty {
        self.flush_c.send(vec![(i, pb.page)]);
      }
    };
    return Ok(());
  }

  pub fn remove(&self, index: usize) {}
}
