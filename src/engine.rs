use std::{path::Path, sync::Arc};

use crate::utils::size;
use crate::Initializer;

use crate::{
  buffer::BufferPool, error::Result, transaction::LockManager, wal::WAL, Cursor,
};

pub struct EngineConfig<T>
where
  T: AsRef<Path>,
{
  pub max_log_size: usize,
  pub max_wal_buffer_size: usize,
  pub checkpoint_interval: usize,
  pub max_cache_size: usize,
  pub base_dir: T,
}

impl Default for EngineConfig<&str> {
  fn default() -> Self {
    Self {
      max_log_size: 2048,
      max_wal_buffer_size: 100,
      checkpoint_interval: 2000,
      max_cache_size: size::mb(512),
      base_dir: "/var/lib/nodb",
    }
  }
}

pub struct Engine {
  wal: Arc<WAL>,
  lock_manager: Arc<LockManager>,
  buffer_pool: Arc<BufferPool>,
}

impl Engine {
  pub fn from_components(
    buffer_pool: Arc<BufferPool>,
    wal: Arc<WAL>,
    lock_manager: Arc<LockManager>,
  ) -> Self {
    Self {
      buffer_pool,
      wal,
      lock_manager,
    }
  }
  pub fn new<T>(config: EngineConfig<T>) -> Result<Self>
  where
    T: AsRef<Path>,
  {
    Initializer::new(config).bootstrap()
  }
}

impl Engine {
  pub fn new_transaction(&self) -> Result<Cursor> {
    let id = self.wal.next_transaction()?;
    let buffer = self.buffer_pool.clone();
    let wal = self.wal.clone();
    let locks = self.lock_manager.clone();
    Ok(Cursor::new(id, buffer, wal, locks))
  }
}
