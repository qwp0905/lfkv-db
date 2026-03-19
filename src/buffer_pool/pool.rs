use std::{
  path::PathBuf,
  sync::{Arc, RwLock},
};

use super::{Acquired, Frame, LRUTable, Peeked, Slot};
use crate::{
  disk::{DiskController, DiskControllerConfig, PagePool, PAGE_SIZE},
  error::Result,
  thread::{BackgroundThread, WorkBuilder, WorkResult},
  utils::{Bitmap, LogFilter, ShortenedMutex, ShortenedRwLock, ToArc, ToBox},
};

pub struct BufferPoolConfig {
  pub shard_count: usize,
  pub capacity: usize,
  pub path: PathBuf,
  pub io_thread_count: usize,
}

pub struct Prefetched(WorkResult<Result<Slot>>);
impl Prefetched {
  pub fn wait(self) -> Result<Slot> {
    self.0.wait_flatten()
  }
}

pub struct BufferPool {
  table: Arc<LRUTable>,
  frame: Arc<Vec<RwLock<Frame>>>,
  dirty: Arc<Bitmap>,
  disk: Arc<DiskController<PAGE_SIZE>>,
  logger: LogFilter,
  prefetch: Box<dyn BackgroundThread<usize, Result<Slot>>>,
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
    let frame = frame.to_arc();

    let table = LRUTable::new(page_pool, config.shard_count, frame_cap).to_arc();
    let dirty = Bitmap::new(config.capacity).to_arc();

    let prefetch = WorkBuilder::new()
      .name("buffer pool prefetch")
      .stack_size(2 << 20)
      .multi(3)
      .shared(handle_prefetch(
        table.clone(),
        disk.clone(),
        frame.clone(),
        dirty.clone(),
      ))
      .to_box();

    Ok(Self {
      frame,
      disk,
      table,
      dirty,
      logger,
      prefetch,
    })
  }

  pub fn peek(&self, index: usize) -> Result<Slot> {
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

  pub fn read(&self, index: usize) -> Result<Slot> {
    buffer_pool_read(&self.table, &self.disk, &self.frame, &self.dirty, index)
  }
  pub fn prefetch(&self, index: usize) -> Prefetched {
    Prefetched(self.prefetch.send(index))
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
    self.prefetch.close();
    self.disk.close();
  }

  pub fn disk_len(&self) -> Result<usize> {
    self.disk.len()
  }
}

#[inline]
fn buffer_pool_read(
  table: &LRUTable,
  disk: &DiskController<PAGE_SIZE>,
  frame: &Vec<RwLock<Frame>>,
  dirty: &Bitmap,
  index: usize,
) -> Result<Slot> {
  let mut guard = match table.acquire(index) {
    Acquired::Temp(temp) => {
      let (state, shard) = temp.take();
      return Ok(Slot::temp(state, index, disk, shard, false));
    }
    Acquired::Hit(state) => {
      let id = state.get_frame_id();
      return Ok(Slot::page(&frame[id], dirty, state));
    }
    Acquired::Evicted(guard) => guard,
  };

  let id = guard.get_frame_id();
  let frame = &frame[id];
  let slot = Slot::page(frame, dirty, guard.get_state());

  {
    let mut page = frame.wl();
    let old = page.replace(index, disk.read(index)?);

    guard
      .get_evicted_index()
      .and_then(|i| dirty.remove(id).then(|| i))
      .map(|evicted| disk.write(evicted, &old))
      .unwrap_or(Ok(()))?;
  }

  guard.commit();
  Ok(slot)
}
fn handle_prefetch(
  table: Arc<LRUTable>,
  disk: Arc<DiskController<PAGE_SIZE>>,
  frame: Arc<Vec<RwLock<Frame>>>,
  dirty: Arc<Bitmap>,
) -> impl Fn(usize) -> Result<Slot> {
  move |index| buffer_pool_read(&table, &disk, &frame, &dirty, index)
}
