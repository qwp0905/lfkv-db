use std::{path::Path, sync::Arc, time::Duration};

use crate::{
  buffer::BufferPool, transaction::LockManager, wal::WAL, Engine, EngineConfig,
  Error, Result, Serializable,
};

use super::{LeafNode, Node, TreeHeader, HEADER_INDEX};

static WAL_FILE: &str = "log.wal";
static DB_FILE: &str = "nodb";

pub struct Initializer<T>
where
  T: AsRef<Path>,
{
  config: EngineConfig<T>,
}
impl<T> Initializer<T>
where
  T: AsRef<Path>,
{
  pub fn new(config: EngineConfig<T>) -> Self {
    Self { config }
  }

  pub fn bootstrap(self) -> Result<Engine>
  where
    T: AsRef<Path>,
  {
    let wal_path = Path::join(self.config.base_dir.as_ref(), WAL_FILE);
    let db_path = Path::join(self.config.base_dir.as_ref(), DB_FILE);

    let lock_manager = Arc::new(LockManager::new());
    let (buffer_pool, flush_c) = BufferPool::open(
      db_path,
      self.config.max_cache_size,
      lock_manager.clone(),
    )?;
    let buffer_pool = Arc::new(buffer_pool);

    let wal = Arc::new(WAL::new(
      wal_path,
      flush_c,
      Duration::from_millis(self.config.checkpoint_interval as u64),
      self.config.max_log_size,
      self.config.max_wal_buffer_size,
    )?);
    match buffer_pool.get(HEADER_INDEX) {
      Ok(_) => wal.replay()?,
      Err(Error::NotFound) => {
        let header = TreeHeader::initial_state();
        let root = Node::Leaf(LeafNode::empty());
        buffer_pool.insert(HEADER_INDEX, header.serialize()?)?;
        buffer_pool.insert(header.get_root(), root.serialize()?)?;
      }
      Err(err) => return Err(err),
    };

    Ok(Engine::from_components(buffer_pool, wal, lock_manager))
  }
}
