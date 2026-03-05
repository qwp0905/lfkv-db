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
  utils::{LogFilter, LogLevel, Logger, ToArc},
  wal::WALConfig,
};

pub struct EngineConfig<T>
where
  T: AsRef<Path>,
{
  pub base_path: T,
  pub wal_file_size: usize,
  pub wal_segment_flush_delay: Duration,
  pub wal_segment_flush_count: usize,
  pub checkpoint_interval: Duration,
  pub group_commit_delay: Duration,
  pub group_commit_count: usize,
  pub gc_trigger_interval: Duration,
  pub gc_thread_count: usize,
  pub buffer_pool_shard_count: usize,
  pub buffer_pool_memory_capacity: usize,
  pub io_thread_count: usize,
  pub logger: Arc<dyn Logger>,
  pub log_level: LogLevel,
}

pub struct Engine {
  orchestrator: Arc<TxOrchestrator>,
  available: AtomicBool,
  logger: LogFilter,
}
impl Engine {
  pub fn bootstrap<T>(config: EngineConfig<T>) -> Result<Self>
  where
    T: AsRef<Path>,
  {
    let logger = LogFilter::new(config.log_level, config.logger);
    logger.info("start engine");

    fs::create_dir_all(config.base_path.as_ref()).map_err(Error::IO)?;
    let wal_config = WALConfig {
      prefix: "wal".into(),
      checkpoint_interval: config.checkpoint_interval,
      group_commit_delay: config.group_commit_delay,
      group_commit_count: config.group_commit_count,
      max_file_size: config.wal_file_size,
      base_dir: config.base_path.as_ref().into(),
      segment_flush_count: config.wal_segment_flush_count,
      segment_flush_delay: config.wal_segment_flush_delay,
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
      thread_count: config.gc_thread_count,
    };
    let orchestrator =
      TxOrchestrator::new(buffer_pool_config, wal_config, gc_config, logger.clone())?
        .to_arc();

    initialize(orchestrator.clone())?;

    logger.info("engine initialized.");
    Ok(Self {
      orchestrator,
      available: AtomicBool::new(true),
      logger,
    })
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
      self.logger.info("engine shutdown");
      if let Err(err) = self.orchestrator.close() {
        self.logger.error(err.to_string());
      };
    }
  }
}
