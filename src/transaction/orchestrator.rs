use std::{
  sync::{Arc, RwLock},
  time::Duration,
};

use super::VersionVisibility;

use crate::{
  buffer_pool::{BufferPool, BufferPoolConfig, PageSlot, PageSlotWrite},
  error::{Error, Result},
  thread::{SingleWorkThread, WorkBuilder},
  transaction::FreeList,
  utils::{ShortenedRwLock, ToArc, ToArcRwLock},
  wal::{WALConfig, WAL},
  GarbageCollectionConfig, GarbageCollector,
};

pub struct TxOrchestrator {
  wal: Arc<WAL>,
  buffer_pool: Arc<BufferPool>,
  checkpoint: Arc<SingleWorkThread<(), Result>>,
  free_list: Arc<FreeList>,
  version_visibility: Arc<VersionVisibility>,
  gc: Arc<GarbageCollector>,
}
impl TxOrchestrator {
  pub fn new(
    buffer_pool_config: BufferPoolConfig,
    wal_config: WALConfig,
    gc_config: GarbageCollectionConfig,
  ) -> Result<Self> {
    let buffer_pool = BufferPool::open(buffer_pool_config)?.to_arc();
    let (wal, last_tx_id, last_free, aborted, redo) = WAL::replay(wal_config)?;
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
    let version_visibility = VersionVisibility::new(aborted, last_tx_id).to_arc();

    let free_list =
      FreeList::new(last_free.clone(), buffer_pool.clone(), wal.clone()).to_arc();

    let gc = GarbageCollector::start(
      buffer_pool.clone(),
      version_visibility.clone(),
      free_list.clone(),
      gc_config,
    )
    .to_arc();

    let checkpoint = WorkBuilder::new()
      .name("")
      .stack_size(1)
      .single()
      .with_timeout(
        Duration::new(1, 1),
        handle_checkpoint(
          wal.clone(),
          buffer_pool.clone(),
          last_free.clone(),
          gc.clone(),
        ),
      )
      .to_arc();

    Ok(Self {
      checkpoint,
      wal,
      free_list,
      buffer_pool,
      version_visibility,
      gc,
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
    self.free_list.alloc()
  }

  pub fn start_tx(&self) -> Result<usize> {
    let tx_id = self.version_visibility.new_transaction();
    match self.wal.append_start(tx_id) {
      Err(Error::WALCapacityExceeded) => self.checkpoint.send_await(())??,
      _ => return Ok(tx_id),
    }
    self.wal.append_start(tx_id)?;
    Ok(tx_id)
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
    self.version_visibility.deactive(&tx_id);
    self.gc.notify();
    Ok(())
  }

  pub fn abort_tx(&self, tx_id: usize) -> Result {
    match self.wal.append_abort(tx_id) {
      Err(Error::WALCapacityExceeded) => self.checkpoint.send_await(())??,
      result => return result,
    }
    self.wal.append_abort(tx_id)?;
    self.version_visibility.move_to_abort(tx_id);
    Ok(())
  }

  pub fn is_visible(&self, tx_id: &usize) -> bool {
    self.version_visibility.is_visible(tx_id)
  }

  pub fn is_active(&self, tx_id: &usize) -> bool {
    self.version_visibility.is_active(tx_id)
  }

  pub fn current_version(&self) -> usize {
    self.version_visibility.current_version()
  }

  pub fn remove_aborted(&self, version: &usize) {
    self.version_visibility.remove_aborted(version);
  }

  pub fn is_aborted(&self, tx_id: &usize) -> bool {
    self.version_visibility.is_aborted(tx_id)
  }
}

fn handle_checkpoint(
  wal: Arc<WAL>,
  buffer_pool: Arc<BufferPool>,
  last_free: Arc<RwLock<usize>>,
  gc: Arc<GarbageCollector>,
) -> impl Fn(Option<()>) -> Result {
  move |_| {
    let log_id = wal.current_log_id();
    gc.run()?;
    buffer_pool.flush()?;
    match wal.append_checkpoint(*last_free.rl(), log_id) {
      Ok(_) => {}
      Err(Error::WALCapacityExceeded) => {}
      Err(err) => return Err(err),
    };
    wal.flush()
  }
}
