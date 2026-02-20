use std::{
  fs,
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
  transaction::TxOrchestrator,
  utils::{logger, ToArc},
  wal::WALConfig,
  Cursor, Error, GarbageCollectionConfig, Result,
};

pub struct EngineConfig<T>
where
  T: AsRef<Path>,
{
  pub base_path: T,
  pub disk_batch_delay: Duration,
  pub disk_batch_size: usize,
  pub defragmentation_interval: Duration,
  pub wal_file_size: usize,
  pub checkpoint_interval: Duration,
  pub group_commit_delay: Duration,
  pub group_commit_count: usize,
  pub gc_trigger_interval: Duration,
  pub gc_trigger_count: usize,
}

const WAL_PATH: &str = "wal.db";
const DISK_PATH: &str = "data.db";

pub struct Engine {
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

    let wal_config = WALConfig {
      path: todo!(),
      max_buffer_size: todo!(),
      checkpoint_interval: todo!(),
      checkpoint_count: todo!(),
      group_commit_delay: todo!(),
      group_commit_count: todo!(),
      max_file_size: todo!(),
    };
    let buffer_pool_config = BufferPoolConfig {
      shard_count: todo!(),
      capacity: todo!(),
      path: todo!(),
      read_threads: todo!(),
      write_threads: todo!(),
    };
    let gc_config = GarbageCollectionConfig {
      interval: todo!(),
      count: todo!(),
    };
    // let garbage_collector = GarbageCollector::start(
    //   orchestrator.clone(),
    //   config.gc_trigger_interval,
    //   config.gc_trigger_count,
    // )
    // .to_arc();
    let orchestrator =
      TxOrchestrator::new(buffer_pool_config, wal_config, gc_config)?.to_arc();

    Cursor::initialize(orchestrator.clone())?;

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
    Ok(Cursor::new(
      self.orchestrator.clone(),
      // self.garbage_collector.clone(),
      tx_id,
    ))
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
