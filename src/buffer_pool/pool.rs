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
  pub io_thread_count: usize,
}

pub struct BufferPool {
  table: LRUTable,
  frame: Vec<RwLock<PageRef<PAGE_SIZE>>>,
  dirty: Bitmap,
  disk: Arc<DiskController<PAGE_SIZE>>,
}
impl BufferPool {
  pub fn open(config: BufferPoolConfig) -> Result<Self> {
    let disk_config = DiskControllerConfig {
      path: config.path,
      thread_count: config.io_thread_count,
    };
    let page_pool = PagePool::new(config.capacity).to_arc();
    let disk = DiskController::open(disk_config, page_pool.clone())?.to_arc();

    let frame_cap = (config.capacity * 9) / 10; // 90% of page pool capacity 10% buffer for disk io
    let mut frame = Vec::with_capacity(frame_cap);
    frame.resize_with(frame_cap, || RwLock::new(page_pool.acquire()));

    Ok(Self {
      frame,
      disk,
      table: LRUTable::new(config.shard_count, frame_cap),
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
    let mut waits = Vec::new();
    for id in self.dirty.iter() {
      let page = self.frame[id].rl();
      let done = self.disk.write_async(self.table.get_index(id), &page);
      waits.push((done, id))
    }

    for (done, id) in waits {
      done.wait()?;
      self.dirty.remove(id);
    }
    self.disk.fsync()?;
    Ok(())
  }

  pub fn is_empty(&self) -> Result<bool> {
    Ok(self.disk_len()? == 0)
  }

  pub fn close(&self) {
    self.disk.close();
  }

  pub fn disk_len(&self) -> Result<usize> {
    self.disk.len()
  }
}
