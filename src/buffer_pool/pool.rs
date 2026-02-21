use std::{
  mem::replace,
  path::PathBuf,
  sync::{Arc, RwLock},
};

use super::{LRUTable, PageSlot};
use crate::{
  disk::{DiskController, DiskControllerConfig, PagePool, PageRef, PAGE_SIZE},
  error::Result,
  utils::{Bitmap, ShortenedRwLock, ToArc},
};

pub struct BufferPoolConfig {
  pub shard_count: usize,
  pub capacity: usize,
  pub path: PathBuf,
  pub read_threads: Option<usize>,
  pub write_threads: Option<usize>,
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
    let page_pool = PagePool::new(config.capacity).to_arc();
    let disk = DiskController::open(dc, page_pool.clone())?.to_arc();

    let mut frame = Vec::with_capacity((config.capacity << 1) / 3);
    frame.resize_with((config.capacity << 1) / 3, || {
      RwLock::new(page_pool.acquire())
    });
    Ok(Self {
      frame,
      disk,
      table: LRUTable::new(config.shard_count, config.capacity),
      dirty: Bitmap::new(config.capacity),
    })
  }

  pub fn read(&self, index: usize) -> Result<PageSlot<'_>> {
    let evicted = match self.table.acquire(index) {
      Ok(id) => return Ok(PageSlot::new(&self.frame[id], id, &self.dirty, index)),
      Err(evicted) => evicted,
    };

    let id = evicted.get_frame_id();
    let mut slot = self.frame[id].wl();
    let page = self.disk.read(index)?;
    let prev = replace(&mut slot as &mut PageRef<PAGE_SIZE>, page);
    let cached = PageSlot::new(&self.frame[id], id, &self.dirty, index);
    let evicted_index = match evicted.get_evicted_index() {
      Some(index) => index,
      None => return Ok(cached),
    };
    if !self.dirty.remove(evicted_index) {
      return Ok(cached);
    }

    self.disk.write(evicted_index, &prev)?;
    Ok(cached)
  }

  pub fn flush(&self) -> Result {
    for id in self.dirty.iter() {
      let page = self.frame[id].rl();
      self.disk.write(self.table.get_index(id), &page)?;
      self.dirty.remove(id);
    }
    self.disk.fsync()?;
    Ok(())
  }

  pub fn is_empty(&self) -> Result<bool> {
    Ok(self.disk.len()? == 0)
  }

  pub fn close(&self) {
    self.disk.close();
  }
}
