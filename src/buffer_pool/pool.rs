use std::{
  path::PathBuf,
  sync::{Arc, RwLock},
};

use super::{Acquired, Frame, LRUTable, Peeked, Slot};
use crate::{
  disk::{DiskController, DiskControllerConfig, PagePool, PAGE_SIZE},
  error::Result,
  utils::{Bitmap, LogFilter, ShortenedMutex, ShortenedRwLock, ToArc},
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

    Ok(Self {
      frame,
      disk,
      table: LRUTable::new(page_pool, config.shard_count, frame_cap),
      dirty: Bitmap::new(config.capacity),
      logger,
    })
  }

  pub fn peek(&self, index: usize) -> Result<Slot<'_>> {
    let (state, shard) = match self.table.peek_or_temp(index) {
      Peeked::Hit(state) => {
        let id = state.get_frame_id();
        return Ok(Slot::page(&self.frame[id], &self.dirty, state));
      }
      Peeked::Temp(temp) => {
        let (state, shard) = temp.take();
        return Ok(Slot::temp(state, index, &self.disk, shard, false));
      }
      Peeked::DiskRead(temp) => temp.take(),
    };
    let page = match self.disk.read(index) {
      Ok(p) => p,
      Err(err) => {
        shard.l().remove_temp(index);
        return Err(err);
      }
    };
    state.for_write().as_mut().copy_from(page.as_ref());
    state.completion_evict(1);
    Ok(Slot::temp(state, index, &self.disk, shard, true))
  }

  pub fn read(&self, index: usize) -> Result<Slot<'_>> {
    let mut guard = match self.table.acquire(index) {
      Acquired::Temp(temp) => {
        let (state, shard) = temp.take();
        return Ok(Slot::temp(state, index, &self.disk, shard, false));
      }
      Acquired::Hit(state) => {
        let id = state.get_frame_id();
        return Ok(Slot::page(&self.frame[id], &self.dirty, state));
      }
      Acquired::Evicted(guard) => guard,
    };

    let id = guard.get_frame_id();
    let frame = &self.frame[id];
    let slot = Slot::page(frame, &self.dirty, guard.get_state());

    {
      let mut page = frame.wl();
      let old = page.replace(index, self.disk.read(index)?);

      guard
        .get_evicted_index()
        .and_then(|i| self.dirty.remove(id).then(|| i))
        .map(|evicted| self.disk.write(evicted, &old))
        .unwrap_or(Ok(()))?;
    }

    guard.commit();
    Ok(slot)
  }

  pub fn flush(&self) -> Result {
    self.logger.debug("buffer pool flush triggered.");
    let mut waits = Vec::new();
    for id in self.dirty.iter() {
      let frame = self.frame[id].rl();
      self.dirty.remove(id);
      waits.push(self.disk.write_async(frame.get_index(), frame.page_ref()));
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
