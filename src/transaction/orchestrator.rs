use std::{
  mem::replace,
  sync::{Arc, RwLock},
  time::Duration,
};

use crate::{
  buffer_pool::{BufferPoolConfig, PageSlotWrite},
  utils::{ShortenedRwLock, ToArcRwLock},
  wal::{WALConfig, WAL},
  BufferPool, Error, PageSlot, Result, SingleWorkThread, ToArc, WorkBuilder,
};

pub struct TxOrchestrator {
  wal: Arc<WAL>,
  buffer_pool: Arc<BufferPool>,
  checkpoint: SingleWorkThread<(), Result>,
  last_free: Arc<RwLock<usize>>,
}
impl TxOrchestrator {
  pub fn new(
    buffer_pool_config: BufferPoolConfig,
    wal_config: WALConfig,
  ) -> Result<Self> {
    let buffer_pool = BufferPool::open(buffer_pool_config)?.to_arc();
    let (wal, last_free, redo) = WAL::replay(wal_config)?;
    let wal = wal.to_arc();
    let last_free = last_free.to_arc_rwlock();
    for (_, i, page) in redo {
      buffer_pool
        .read(i)?
        .for_write()
        .as_mut()
        .writer()
        .write(page.as_ref())?;
    }

    let checkpoint = WorkBuilder::new()
      .name("")
      .stack_size(1)
      .single()
      .with_timeout(
        Duration::new(1, 1),
        handle_checkpoint(wal.clone(), buffer_pool.clone(), last_free.clone()),
      );

    Ok(Self {
      checkpoint,
      last_free,
      wal,
      buffer_pool,
    })
  }
  pub fn fetch(&self, index: usize) -> Result<PageSlot<'_>> {
    self.buffer_pool.read(index)
  }
  pub fn log(&self, tx_id: usize, page: &PageSlotWrite<'_>) -> Result {
    let index = page.get_index();
    match self.wal.append_insert(tx_id, index, page.as_ref()) {
      Err(Error::WALCapacityExceeded) => self.checkpoint.send_await(())??,
      result => return result,
    };
    self.wal.append_insert(tx_id, index, page.as_ref())
  }
  pub fn alloc(&self) -> Result<PageSlot<'_>> {
    let mut index = self.last_free.wl();
    let slot = self.buffer_pool.read(*index)?;
    let next = slot.for_read().as_ref().scanner().read_usize()?;
    match self.wal.append_free(next) {
      Ok(_) => {}
      Err(Error::WALCapacityExceeded) => self
        .checkpoint
        .send_await(())?
        .and_then(|_| self.wal.append_free(next))?,
      Err(err) => return Err(err),
    };
    *index = next;
    Ok(slot)
  }
  pub fn release(&self, page: PageSlot<'_>) -> Result {
    let index = page.get_index();
    let mut prev = self.last_free.wl();
    match self.wal.append_free(index) {
      Ok(_) => {}
      Err(Error::WALCapacityExceeded) => self
        .checkpoint
        .send_await(())?
        .and_then(|_| self.wal.append_free(index))?,
      Err(err) => return Err(err),
    };
    let _ = page
      .for_write()
      .as_mut()
      .writer()
      .write_usize(replace(&mut prev, index));
    Ok(())
  }

  pub fn start_tx(&self) -> Result<usize> {
    match self.wal.append_start() {
      Err(Error::WALCapacityExceeded) => self.checkpoint.send_await(())??,
      result => return result,
    }
    self.wal.append_start()
  }

  pub fn commit_tx(&self, tx_id: usize) -> Result {
    match self.wal.append_commit(tx_id) {
      Ok(_) => {}
      Err(Error::WALCapacityExceeded) => self
        .checkpoint
        .send_await(())?
        .and_then(|_| self.wal.append_commit(tx_id))?,
      Err(err) => return Err(err),
    }
    self.wal.flush()?;

    // mark tx id committed
    Ok(())
  }

  pub fn abort_tx(&self, tx_id: usize) -> Result {
    match self.wal.append_abort(tx_id) {
      Err(Error::WALCapacityExceeded) => self.checkpoint.send_await(())??,
      result => return result,
    }
    self.wal.append_abort(tx_id)
  }

  pub fn is_available(&self, tx_id: usize) -> bool {
    // version visibility check
    true
  }
}

fn handle_checkpoint(
  wal: Arc<WAL>,
  buffer_pool: Arc<BufferPool>,
  last_free: Arc<RwLock<usize>>,
) -> impl Fn(Option<()>) -> Result {
  move |_| {
    buffer_pool.flush()?;
    match wal.append_checkpoint(*last_free.rl()) {
      Ok(_) => {}
      Err(Error::WALCapacityExceeded) => {}
      Err(err) => return Err(err),
    };
    wal.flush()
  }
}
