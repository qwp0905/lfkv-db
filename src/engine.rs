use std::{path::Path, sync::Arc, time::Duration};

use crate::utils::size;

use crate::{
  buffer::BufferPool, error::Result, transaction::LockManager, wal::WAL, Cursor,
};

static WAL_FILE: &str = "log.wal";
static DB_FILE: &str = "nodb";

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
      max_cache_size: size::mb(500),
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
  pub fn new<T>(config: EngineConfig<T>) -> Result<Self>
  where
    T: AsRef<Path>,
  {
    let wal_path = Path::join(config.base_dir.as_ref(), WAL_FILE);
    let db_path = Path::join(config.base_dir.as_ref(), DB_FILE);

    let lock_manager = Arc::new(LockManager::new());
    let (buffer_pool, flush_c) =
      BufferPool::open(db_path, config.max_cache_size, lock_manager.clone())?;
    let buffer_pool = Arc::new(buffer_pool);

    let wal = Arc::new(WAL::new(
      wal_path,
      flush_c,
      Duration::from_millis(config.checkpoint_interval as u64),
      config.max_log_size,
      config.max_wal_buffer_size,
    )?);
    Ok(Self {
      wal,
      lock_manager,
      buffer_pool,
    })
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
