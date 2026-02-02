use std::sync::Mutex;

use crate::{DrainAll, ShortenedMutex};

use super::record::{LogEntry, LogRecord};

struct LogBufferInner {
  buffer: Vec<LogEntry>,
  max_size: usize,
  cursor: usize,
}
pub struct LogBuffer(Mutex<LogBufferInner>);
impl LogBuffer {
  pub fn new(max_size: usize) -> Self {
    // Self(Mutex::new()
    todo!()
  }

  pub fn append(&self, record: LogRecord) -> () {}
}
