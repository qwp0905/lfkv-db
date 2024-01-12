use std::sync::Arc;

use crate::{
  buffer::BufferPool, error::Result, transaction::TransactionManager, wal::WAL,
};

pub struct EngineConfig {
  max_log_size: usize,
  max_buffer_size: usize,
}

pub struct Engine {
  wal: Arc<WAL>,
  transactions: Arc<TransactionManager>,
  buffer_pool: BufferPool,
}

// impl Engine {
//   pub fn start_new() -> Result<Self> {}
// }
