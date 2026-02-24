use std::{
  fs,
  path::Path,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::Duration,
};

use super::constant::{DATA_PATH, FILE_SUFFIX};
use crate::{
  buffer_pool::BufferPoolConfig,
  cursor::{initialize, Cursor, GarbageCollectionConfig},
  disk::PAGE_SIZE,
  error::{Error, Result},
  transaction::TxOrchestrator,
  utils::{logger, ToArc},
  wal::WALConfig,
};

pub struct EngineConfig<T>
where
  T: AsRef<Path>,
{
  pub base_path: T,
  pub wal_file_size: usize,
  pub checkpoint_interval: Duration,
  pub group_commit_delay: Duration,
  pub group_commit_count: usize,
  pub gc_trigger_interval: Duration,
  pub gc_trigger_count: usize,
  pub buffer_pool_shard_count: usize,
  pub buffer_pool_memory_capacity: usize,
  pub io_thread_count: usize,
}

pub struct Engine {
  orchestrator: Arc<TxOrchestrator>,
  available: AtomicBool,
}
impl Engine {
  pub fn bootstrap<T>(config: EngineConfig<T>) -> Result<Self>
  where
    T: AsRef<Path>,
  {
    fs::create_dir_all(config.base_path.as_ref()).map_err(Error::IO)?;

    let wal_config = WALConfig {
      prefix: "wal".into(),
      checkpoint_interval: config.checkpoint_interval,
      group_commit_delay: config.group_commit_delay,
      group_commit_count: config.group_commit_count,
      max_file_size: config.wal_file_size,
      base_dir: config.base_path.as_ref().into(),
    };
    let buffer_pool_config = BufferPoolConfig {
      shard_count: config.buffer_pool_shard_count,
      capacity: config.buffer_pool_memory_capacity / PAGE_SIZE,
      path: config
        .base_path
        .as_ref()
        .join(format!("{}{}", DATA_PATH, FILE_SUFFIX)),
      io_thread_count: config.io_thread_count,
    };
    let gc_config = GarbageCollectionConfig {
      interval: config.gc_trigger_interval,
      count: config.gc_trigger_count,
    };
    let orchestrator =
      TxOrchestrator::new(buffer_pool_config, wal_config, gc_config)?.to_arc();

    initialize(orchestrator.clone())?;

    let engine = Self {
      orchestrator,
      available: AtomicBool::new(true),
    };

    logger::info("engine initialized");
    Ok(engine)
  }

  pub fn new_transaction(&self) -> Result<Cursor> {
    if !self.available.load(Ordering::Acquire) {
      return Err(Error::EngineUnavailable);
    }
    let tx_id = self.orchestrator.start_tx()?;
    Ok(Cursor::new(self.orchestrator.clone(), tx_id))
  }
}

impl Drop for Engine {
  fn drop(&mut self) {
    if let Ok(_) =
      self
        .available
        .compare_exchange(true, false, Ordering::Release, Ordering::Acquire)
    {
      logger::info("engine shutdown");
      if let Err(err) = self.orchestrator.close() {
        logger::error(err);
      };
    }
  }
}
