use std::{sync::RwLock, time::Duration};

use crate::Page;

use super::{LogRecord, LogWriter};

pub struct LogStorage {
  core: RwLock<LogStorageCore>,
}

struct LogStorageCore {
  buffer: Vec<LogRecord>,
  last_index: usize,
  last_transaction: usize,
  checkpoint_count: usize,
  checkpoint_interval: Duration,
  writer: LogWriter,
}
impl LogStorageCore {
  fn append(&mut self, tx_id: usize, page_index: usize, data: Page) {
    let index = self.last_index + 1;
    let log = LogRecord::new_insert(index, tx_id, page_index, data);
    self.buffer.push(log);
  }

  fn new_transaction(&mut self) {
    self.last_index += 1;
    self.last_transaction += 1;
    let record = LogRecord::new_start(self.last_index, self.last_transaction);
    self.buffer.push(record);
  }

  fn commit(&mut self, tx_id: usize) {
    let mut committed = vec![];
    self.buffer = self.buffer.drain(..).fold(vec![], |mut a, r| {
      if r.transaction_id != tx_id {
        a.push(r);
      } else {
        committed.push(r);
      };
      return a;
    });
  }
}
