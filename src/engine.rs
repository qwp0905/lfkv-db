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
  buffer::{BufferPool, RollbackStorage, RollbackStorageConfig, BLOCK_SIZE},
  disk::{Finder, FinderConfig, FreeList},
  logger,
  wal::{WriteAheadLog, WriteAheadLogConfig},
  Cursor, Error, Result,
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
  wal: Arc<WriteAheadLog>,
  buffer_pool: Arc<BufferPool>,
  freelist: Arc<FreeList<BLOCK_SIZE>>,
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

    let disk = Arc::new(Finder::open(FinderConfig {
      path: config.base_path.as_ref().join(DISK_PATH),
      read_threads: None,
      write_threads: None,
    })?);
    logger::info(format!("disk created"));

    let freelist = Arc::new(FreeList::new(
      config.defragmentation_interval,
      disk.clone(),
    )?);
    logger::info(format!("freelist created"));

    let rollback = Arc::new(RollbackStorage::open(RollbackStorageConfig {
      fsync_delay: config.undo_batch_delay,
      fsync_count: config.undo_batch_size,
      max_cache_size: mem_size.div_ceil(10),
      max_file_size: config.undo_file_size,
      path: config.base_path.as_ref().join(UNDO_PATH),
    })?);
    logger::info(format!("undo log created"));

    let (bp, flush_c, commit_c) =
      BufferPool::generate(rollback, disk, mem_size.div_ceil(10).mul(3));
    let buffer_pool = Arc::new(bp);
    logger::info(format!("buffer pool created"));

    let wal = Arc::new(WriteAheadLog::open(
      WriteAheadLogConfig {
        path: config.base_path.as_ref().join(WAL_PATH),
        max_buffer_size: mem_size.div_ceil(10).mul(1),
        checkpoint_interval: config.checkpoint_interval,
        checkpoint_count: config.checkpoint_count,
        group_commit_delay: config.group_commit_delay,
        group_commit_count: config.group_commit_count,
        max_file_size: config.wal_file_size,
      },
      Arc::new(commit_c),
      flush_c,
      &buffer_pool,
    )?);
    logger::info("wal created");

    let engine = Self {
      wal,
      buffer_pool,
      freelist,
      available: AtomicBool::new(true),
    };

    let cursor = engine.new_transaction()?;
    cursor.initialize()?;
    cursor.commit()?;

    logger::info("engine initialized");
    Ok(engine)
  }

  pub fn new_transaction(&self) -> Result<Cursor> {
    if !self.available.load(Ordering::SeqCst) {
      return Err(Error::EngineUnavailable);
    }

    Cursor::new(
      self.freelist.clone(),
      self.wal.clone(),
      self.buffer_pool.clone(),
    )
  }
}

impl Drop for Engine {
  fn drop(&mut self) {
    self.available.store(false, Ordering::SeqCst);
    self.wal.before_shutdown();
    self.buffer_pool.before_shutdown();
    self.freelist.before_shutdown();
  }
}
