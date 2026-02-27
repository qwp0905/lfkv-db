use std::{sync::Arc, time::Duration};

use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError};

use super::VersionVisibility;

use crate::{
  buffer_pool::{BufferPool, BufferPoolConfig, PageSlot, PageSlotWrite},
  cursor::{GarbageCollectionConfig, GarbageCollector},
  error::Result,
  thread::{SingleWorkThread, WorkBuilder},
  transaction::FreeList,
  utils::{logger, ToArc},
  wal::{WALConfig, WALSegment, WAL},
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
    logger::info("trying to open buffer pool");
    let buffer_pool = BufferPool::open(buffer_pool_config)?.to_arc();
    let checkpoint_interval = wal_config.checkpoint_interval;
    let (checkpoint_queue, recv) = unbounded();

    let (wal, last_tx_id, last_free, aborted, redo, segments) =
      WAL::replay(wal_config, checkpoint_queue)?;
    let wal = wal.to_arc();
    for (_, i, page) in redo {
      buffer_pool
        .read(i)?
        .for_write()
        .as_mut()
        .writer()
        .write(page.as_ref())?;
    }
    buffer_pool.flush()?;

    let version_visibility = VersionVisibility::new(aborted, last_tx_id).to_arc();
    let disk_len = buffer_pool.disk_len()?;

    let free_list =
      FreeList::new(last_free, disk_len, buffer_pool.clone(), wal.clone()).to_arc();

    let gc = GarbageCollector::start(
      buffer_pool.clone(),
      version_visibility.clone(),
      free_list.clone(),
      gc_config,
    )
    .to_arc();

    if disk_len != 0 {
      logger::info("create initial checkpoint");
      let log_id = wal.current_log_id();
      gc.run()?;
      buffer_pool.flush()?;
      wal.append_checkpoint(free_list.get_last_free(), log_id)?;
      wal.flush()?;

      for seg in segments {
        seg.unlink()?;
      }
    }

    let checkpoint = WorkBuilder::new()
      .name("checkpoint")
      .stack_size(2 << 20)
      .single()
      .no_timeout(handle_checkpoint(
        wal.clone(),
        buffer_pool.clone(),
        free_list.clone(),
        gc.clone(),
        recv,
        checkpoint_interval,
      ))
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
    self.wal.append_insert(tx_id, index, page.as_ref())?;
    Ok(())
  }

  pub fn alloc(&self) -> Result<PageSlot<'_>> {
    self.free_list.alloc()
  }

  pub fn start_tx(&self) -> Result<usize> {
    let tx_id = self.version_visibility.new_transaction();
    self.wal.append_start(tx_id)?;
    Ok(tx_id)
  }

  pub fn commit_tx(&self, tx_id: usize) -> Result {
    self.wal.append_commit(tx_id)?;
    self.wal.flush()?;
    self.version_visibility.deactive(&tx_id);
    Ok(())
  }

  pub fn abort_tx(&self, tx_id: usize) -> Result {
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
  pub fn is_disk_empty(&self) -> Result<bool> {
    self.buffer_pool.is_empty()
  }
  pub fn initial_state(&self, index: usize) {
    self.free_list.set_next_index(index);
  }

  pub fn close(&self) -> Result {
    self.checkpoint.close();
    run_checkpoint(
      None,
      &self.wal,
      &self.buffer_pool,
      &self.free_list,
      &self.gc,
    )?;
    logger::info("last checkpoint completed.");
    self.buffer_pool.close();
    logger::info("buffer pool closed.");
    self.wal.close();
    logger::info("wal closed.");
    Ok(())
  }
}

fn handle_checkpoint(
  wal: Arc<WAL>,
  buffer_pool: Arc<BufferPool>,
  free_list: Arc<FreeList>,
  gc: Arc<GarbageCollector>,
  recv: Receiver<WALSegment>,
  timeout: Duration,
) -> impl Fn(()) -> Result {
  move |_| loop {
    let segment = match recv.recv_timeout(timeout) {
      Ok(v) => Some(v),
      Err(RecvTimeoutError::Timeout) => None,
      Err(_) => return Ok(()),
    };
    let _ = run_checkpoint(segment, &wal, &buffer_pool, &free_list, &gc);
  }
}

fn run_checkpoint(
  segment: Option<WALSegment>,
  wal: &WAL,
  buffer_pool: &BufferPool,
  free_list: &FreeList,
  gc: &GarbageCollector,
) -> Result {
  let r = segment.as_ref().map(|s| s.fsync());
  let log_id = wal.current_log_id();
  gc.run()?;
  buffer_pool.flush()?;
  wal.append_checkpoint(free_list.get_last_free(), log_id)?;
  segment.map(|s| s.unlink()).unwrap_or(Ok(()))?;
  r.map(|f| f.wait()).unwrap_or_else(|| Ok(()))?;
  wal.flush()?;
  Ok(())
}
