use std::{
  mem::replace,
  path::PathBuf,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, RwLock,
  },
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
  pins: Vec<AtomicUsize>,
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
    let mut pins = Vec::with_capacity(frame_cap);
    pins.resize_with(frame_cap, Default::default);

    Ok(Self {
      frame,
      pins,
      disk,
      table: LRUTable::new(config.shard_count, frame_cap),
      dirty: Bitmap::new(config.capacity),
    })
  }

  fn frame_available(&self, frame_id: usize) -> bool {
    self.pins[frame_id]
      .compare_exchange(0, 1, Ordering::Release, Ordering::Acquire)
      .is_ok()
  }

  fn swap_frame_from_disk(
    &self,
    frame_id: usize,
    index: usize,
  ) -> Result<PageRef<PAGE_SIZE>> {
    let mut slot = self.frame[frame_id].wl();
    let replacement = self.disk.read(index)?;
    Ok(replace(&mut slot as &mut PageRef<PAGE_SIZE>, replacement))
  }

  pub fn read(&self, index: usize) -> Result<PageSlot<'_>> {
    let evicted = match self.table.acquire(index, |id| self.frame_available(id)) {
      Ok(guard) => {
        let id = guard.get_frame_id();
        let pin = &self.pins[id];
        pin.fetch_add(1, Ordering::Release);
        return Ok(PageSlot::new(&self.frame[id], id, &self.dirty, index, pin));
      }
      Err(evicted) => evicted,
    };

    let id = evicted.get_frame_id();
    let prev = self.swap_frame_from_disk(id, index)?;

    let pin = &self.pins[id];
    let cached = PageSlot::new(&self.frame[id], id, &self.dirty, index, pin);
    if let Some(evicted_index) = evicted.get_evicted_index() {
      if self.dirty.remove(id) {
        self.disk.write(evicted_index, &prev)?;
      }
      return Ok(cached);
    }

    pin.fetch_add(1, Ordering::Release);
    Ok(cached)
  }

  pub fn flush(&self) -> Result {
    let mut waits = Vec::new();
    for id in self.dirty.iter() {
      let page = self.frame[id].rl();
      self.dirty.remove(id);
      waits.push(self.disk.write_async(self.table.get_index(id), &page));
    }

    for done in waits {
      done.wait()?;
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
