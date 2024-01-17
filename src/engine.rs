use std::time::Duration;
use std::{path::Path, sync::Arc};

use crate::disk::PageSeeker;
use crate::utils::size;
use crate::Error;

use crate::{
  buffer::BufferPool, error::Result, transaction::LockManager, wal::WAL, Cursor,
};

static WAL_FILE: &str = "wal.db";
static DB_FILE: &str = "no.db";

pub struct EngineConfig<T>
where
  T: AsRef<Path>,
{
  pub max_log_size: usize,
  pub max_wal_buffer_size: usize,
  pub checkpoint_interval: Duration,
  pub max_cache_size: usize,
  pub base_dir: T,
}

impl Default for EngineConfig<&str> {
  fn default() -> Self {
    Self {
      max_log_size: size::mb(16),
      max_wal_buffer_size: size::mb(2),
      checkpoint_interval: Duration::from_millis(2000),
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
  fn from_components(
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
  pub fn bootstrap<T>(config: EngineConfig<T>) -> Result<Self>
  where
    T: AsRef<Path>,
  {
    let wal_path = Path::join(config.base_dir.as_ref(), WAL_FILE);
    let db_path = Path::join(config.base_dir.as_ref(), DB_FILE);

    let db = Arc::new(PageSeeker::open(db_path)?);

    let lock_manager = Arc::new(LockManager::new());
    let buffer_pool = Arc::new(BufferPool::new(
      db.clone(),
      config.max_cache_size,
      lock_manager.clone(),
    ));

    let wal = Arc::new(WAL::open(
      wal_path,
      config.max_log_size,
      config.max_wal_buffer_size,
      db,
      config.checkpoint_interval,
    )?);

    wal.replay()?;
    if let Err(Error::NotFound) = buffer_pool.get(0) {
      Cursor::new(
        wal.next_transaction()?,
        buffer_pool.clone(),
        wal.clone(),
        lock_manager.clone(),
      )
      .initialize()?;
    }
    Ok(Self::from_components(buffer_pool, wal, lock_manager))
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
// impl Drop for Engine {
//   fn drop(&mut self) {
//     self.wal.close();
//   }
// }
