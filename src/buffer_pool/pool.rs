use std::{
  path::PathBuf,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, RwLock,
  },
};

use super::{Frame, LRUTable, PageSlot};
use crate::{
  disk::{DiskController, DiskControllerConfig, PagePool, PAGE_SIZE},
  error::Result,
  utils::{Bitmap, LogFilter, ShortenedRwLock, ToArc},
};

pub struct BufferPoolConfig {
  pub shard_count: usize,
  pub capacity: usize,
  pub path: PathBuf,
  pub io_thread_count: usize,
}

pub struct BufferPool {
  table: LRUTable,
  frame: Vec<RwLock<Frame>>,
  pins: Vec<AtomicUsize>,
  dirty: Bitmap,
  disk: Arc<DiskController<PAGE_SIZE>>,
  logger: LogFilter,
}
impl BufferPool {
  pub fn open(config: BufferPoolConfig, logger: LogFilter) -> Result<Self> {
    let disk_config = DiskControllerConfig {
      path: config.path,
      thread_count: config.io_thread_count,
    };
    let page_pool = PagePool::new(config.capacity).to_arc();
    let disk = DiskController::open(disk_config, page_pool.clone())?.to_arc();

    let frame_cap = (config.capacity * 9) / 10; // 90% of page pool capacity 10% buffer for disk io
    let mut frame = Vec::with_capacity(frame_cap);
    frame.resize_with(frame_cap, || RwLock::new(Frame::empty(page_pool.acquire())));

    let mut pins = Vec::with_capacity(frame_cap);
    pins.resize_with(frame_cap, Default::default);

    Ok(Self {
      frame,
      pins,
      disk,
      table: LRUTable::new(config.shard_count, frame_cap),
      dirty: Bitmap::new(config.capacity),
      logger,
    })
  }

  fn frame_available(&self, frame_id: usize) -> bool {
    self.pins[frame_id]
      .compare_exchange(0, 1, Ordering::Release, Ordering::Acquire)
      .is_ok()
  }

  pub fn read(&self, index: usize) -> Result<PageSlot<'_>> {
    let mut guard = self.table.acquire(index, |id| self.frame_available(id));
    if guard.is_succeeded() {
      let id = guard.get_frame_id();
      let pin = &self.pins[id];
      pin.fetch_add(1, Ordering::Release);
      return Ok(PageSlot::new(&self.frame[id], id, &self.dirty, pin));
    }

    let id = guard.get_frame_id();
    let frame = &self.frame[id];
    let pin = &self.pins[id];
    let slot = PageSlot::new(frame, id, &self.dirty, pin);

    let evicted = match guard.get_evicted_index() {
      Some(i) => i,
      None => {
        frame.wl().replace(index, self.disk.read(index)?);
        pin.fetch_add(1, Ordering::Release);
        return Ok(slot);
      }
    };

    if let Err(err) = self
      .disk
      .read(index)
      .map(|page| frame.wl().replace(index, page))
      .and_then(|prev| {
        guard.take();
        if !self.dirty.remove(id) {
          return Ok(());
        }
        self.disk.write(evicted, &prev)
      })
    {
      pin.fetch_sub(1, Ordering::Release);
      return Err(err);
    }

    Ok(slot)
  }

  pub fn flush(&self) -> Result {
    self.logger.debug("buffer pool flush triggered.");
    let mut waits = Vec::new();
    for id in self.dirty.iter() {
      self.pins[id].fetch_add(1, Ordering::Release);
      let frame = self.frame[id].rl();
      self.dirty.remove(id);
      waits.push(self.disk.write_async(frame.get_index(), frame.page_ref()));
      self.pins[id].fetch_sub(1, Ordering::Release);
    }

    waits
      .into_iter()
      .map(|w| w.wait())
      .fold(Ok(()), |a, c| a.and_then(|_| c))?;
    self.logger.debug("buffer pool flushed all pages.");

    self.disk.fsync()?;
    self.logger.debug("buffer pool synced.");
    Ok(())
  }

  pub fn close(&self) {
    self.disk.close();
  }

  pub fn disk_len(&self) -> Result<usize> {
    self.disk.len()
  }
}
