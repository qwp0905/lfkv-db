use std::sync::RwLock;

use super::LogRecord;

pub struct LogStorage {
  core: RwLock<LogStorageCore>,
}

struct LogStorageCore {
  buffer: Vec<LogRecord>,
  last_index: usize,
  last_transaction: usize,
}
