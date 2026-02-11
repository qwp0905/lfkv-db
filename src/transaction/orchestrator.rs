use std::{
  mem::replace,
  sync::{Arc, RwLock},
  time::Duration,
};

use crate::{
  buffer_pool::{BufferPoolConfig, CachedSlotWrite},
  utils::{ShortenedRwLock, ToArcRwLock},
  wal::{WALConfig, WAL},
  BufferPool, CachedSlot, Error, Result, SingleWorkThread, ToArc, WorkBuilder,
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

    let b = buffer_pool.clone();
    let w = wal.clone();
    let f = last_free.clone();
    let checkpoint = WorkBuilder::new()
      .name("")
      .stack_size(1)
      .single()
      .with_timeout(Duration::new(1, 1), move |_| {
        b.flush()?;
        match w.append_checkpoint(*f.rl()) {
          Ok(_) => {}
          Err(Error::WALCapacityExceeded) => {}
          Err(err) => return Err(err),
        };
        w.flush()
      });

    Ok(Self {
      checkpoint,
      last_free,
      wal,
      buffer_pool,
    })
  }
  pub fn fetch(&self, index: usize) -> Result<CachedSlot<'_>> {
    self.buffer_pool.read(index)
  }
  pub fn log(&self, tx_id: usize, page: &CachedSlotWrite<'_>) -> Result {
    let index = page.get_index();
    match self.wal.append_insert(tx_id, index, page.as_ref()) {
      Err(Error::WALCapacityExceeded) => self.checkpoint.send_await(())??,
      result => return result,
    };
    self.wal.append_insert(tx_id, index, page.as_ref())
  }
  pub fn alloc(&self) -> Result<CachedSlot<'_>> {
    let mut index = self.last_free.wl();
    let page = self.buffer_pool.read(*index)?;
    let next = page.for_read().as_ref().scanner().read_usize()?;
    match self.wal.append_free(next) {
      Ok(_) => {}
      Err(Error::WALCapacityExceeded) => self
        .checkpoint
        .send_await(())?
        .and_then(|_| self.wal.append_free(next))?,
      Err(err) => return Err(err),
    };
    *index = next;
    Ok(page)
  }
  pub fn release(&self, page: CachedSlot<'_>) -> Result {
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
