use std::{
  fs,
  ops::Mul,
  path::Path,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::Duration,
};

use sysinfo::System;

use crate::{
  buffer_pool::BufferPoolConfig,
  disk::{DiskController, DiskControllerConfig},
  logger,
  transaction::TxOrchestrator,
  utils::ToArc,
  wal::WALConfig,
  // buffer::{BufferPool, RollbackStorage, RollbackStorageConfig, BLOCK_SIZE},
  Cursor,
  Error,
  Result,
};

pub struct EngineConfig<T>
where
  T: AsRef<Path>,
{
  pub base_path: T,
  pub disk_batch_delay: Duration,
  pub disk_batch_size: usize,
  pub defragmentation_interval: Duration,
  pub undo_batch_delay: Duration,
  pub undo_batch_size: usize,
  pub undo_file_size: usize,
  pub wal_file_size: usize,
  pub checkpoint_interval: Duration,
  pub checkpoint_count: usize,
  pub group_commit_delay: Duration,
  pub group_commit_count: usize,
}

const WAL_PATH: &str = "wal.db";
const UNDO_PATH: &str = "undo.db";
const DISK_PATH: &str = "data.db";

pub struct Engine {
  // wal: Arc<WriteAheadLog>,
  // buffer_pool: Arc<BufferPool>,
  // freelist: Arc<FreeList<BLOCK_SIZE>>,
  orchestrator: Arc<TxOrchestrator>,
  available: AtomicBool,
}
impl Engine {
  pub fn bootstrap<T>(config: EngineConfig<T>) -> Result<Self>
  where
    T: AsRef<Path>,
  {
    let mem_size = System::new_all().total_memory() as usize;
    logger::info(format!("{} system memory", mem_size));
    fs::create_dir_all(config.base_path.as_ref()).map_err(Error::IO)?;

    let wal_config = WALConfig {};
    let buffer_pool_config = BufferPoolConfig {};
    let orchestrator = TxOrchestrator::new(buffer_pool_config, wal_config)?.to_arc();

    let engine = Self {
      orchestrator,
      available: AtomicBool::new(true),
    };

    let cursor = engine.new_transaction()?;
    cursor.initialize()?;
    cursor.commit()?;

    logger::info("engine initialized");
    Ok(engine)
  }

  pub fn new_transaction(&self) -> Result<Cursor> {
    if !self.available.load(Ordering::Release) {
      return Err(Error::EngineUnavailable);
    }
    Ok(Cursor::new(self.orchestrator.clone(), tx_id))
  }
}

impl Drop for Engine {
  fn drop(&mut self) {
    if let Ok(_) =
      self
        .available
        .compare_exchange(true, false, Ordering::Acquire, Ordering::Acquire)
    {
      // self.wal.before_shutdown();
      // self.buffer_pool.before_shutdown();
      // self.freelist.before_shutdown();
    }
  }
}
