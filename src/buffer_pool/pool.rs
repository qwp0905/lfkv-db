use std::{
  mem::replace,
  path::PathBuf,
  sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crate::{
  buffer_pool::table::LRUTable,
  disk::{DiskController, DiskControllerConfig, PagePool, PageRef, PAGE_SIZE},
  utils::{Bitmap, ShortenedRwLock},
  Page, Result, ToArc,
};

pub struct BufferPoolConfig {
  pub shard_count: usize,
  pub capacity: usize,
  pub path: PathBuf,
  pub read_threads: Option<usize>,
  pub write_threads: Option<usize>,
}

pub struct CachedPage<'a> {
  page: &'a RwLock<PageRef<PAGE_SIZE>>,
  frame_id: usize,
  dirty: &'a Bitmap,
  index: usize,
}
impl<'a> CachedPage<'a> {
  fn new(
    page: &'a RwLock<PageRef<PAGE_SIZE>>,
    frame_id: usize,
    dirty: &'a Bitmap,
    index: usize,
  ) -> Self {
    Self {
      page,
      frame_id,
      dirty,
      index,
    }
  }
  pub fn get_index(&self) -> usize {
    self.index
  }

  pub fn for_read(&self) -> CachedPageRead<'a> {
    CachedPageRead {
      guard: self.page.rl(),
    }
  }
  pub fn for_write(&self) -> CachedPageWrite<'a> {
    CachedPageWrite {
      guard: self.page.wl(),
      dirty: self.dirty,
      frame_id: self.frame_id,
      index: self.index,
    }
  }
}
pub struct CachedPageWrite<'a> {
  guard: RwLockWriteGuard<'a, PageRef<PAGE_SIZE>>,
  index: usize,
  dirty: &'a Bitmap,
  frame_id: usize,
}
impl<'a> CachedPageWrite<'a> {
  pub fn get_index(&self) -> usize {
    self.index
  }
}
impl<'a> AsMut<Page<PAGE_SIZE>> for CachedPageWrite<'a> {
  fn as_mut(&mut self) -> &mut Page<PAGE_SIZE> {
    self.guard.as_mut()
  }
}
impl<'a> AsRef<Page<PAGE_SIZE>> for CachedPageWrite<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    self.guard.as_ref()
  }
}
impl<'a> Drop for CachedPageWrite<'a> {
  fn drop(&mut self) {
    self.dirty.insert(self.frame_id);
  }
}
pub struct CachedPageRead<'a> {
  guard: RwLockReadGuard<'a, PageRef<PAGE_SIZE>>,
}
impl<'a> AsRef<Page<PAGE_SIZE>> for CachedPageRead<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    self.guard.as_ref()
  }
}

pub struct BufferPool {
  table: LRUTable,
  frame: Vec<RwLock<PageRef<PAGE_SIZE>>>,
  dirty: Bitmap,
  disk: Arc<DiskController<PAGE_SIZE>>,
}
impl BufferPool {
  pub fn open(config: BufferPoolConfig) -> Result<Self> {
    let dc = DiskControllerConfig {
      path: config.path,
      read_threads: config.read_threads,
      write_threads: config.write_threads,
    };
    let page_pool = PagePool::new(1).to_arc();
    let disk = DiskController::open(dc, page_pool.clone())?.to_arc();

    let mut frame = Vec::with_capacity(config.capacity);
    frame.resize_with(config.capacity, || RwLock::new(page_pool.acquire()));
    Ok(Self {
      frame,
      disk,
      table: LRUTable::new(config.shard_count, config.capacity),
      dirty: Bitmap::new(config.capacity),
    })
  }

  pub fn read(&self, index: usize) -> Result<CachedPage<'_>> {
    let evicted = match self.table.acquire(index) {
      Ok(id) => return Ok(CachedPage::new(&self.frame[id], id, &self.dirty, index)),
      Err(evicted) => evicted,
    };

    let id = evicted.get_frame_id();
    let mut slot = self.frame[id].wl();
    let page = self.disk.read(index)?;
    let prev = replace(&mut slot as &mut PageRef<PAGE_SIZE>, page);
    let cached = CachedPage::new(&self.frame[id], id, &self.dirty, index);
    let evicted_index = match evicted.get_evicted_index() {
      Some(index) => index,
      None => return Ok(cached),
    };
    if !self.dirty.remove(evicted_index) {
      return Ok(cached);
    }

    self.disk.write(index, &prev)?;
    Ok(cached)
  }

  pub fn flush(&self) -> Result {
    for id in self.dirty.iter() {
      let page = self.frame[id].rl();
      self.disk.write(self.table.get_index(id), &page)?;
      self.dirty.remove(id);
    }
    Ok(())
  }
}
unsafe impl Send for BufferPool {}
unsafe impl Sync for BufferPool {}
