use std::{
  sync::{Arc, Mutex},
  time::Duration,
};

use crossbeam::channel::Receiver;

use crate::{DroppableReceiver, Page, ShortenedMutex, StoppableChannel};

use super::{LogBuffer, LogRecord, LogWriter, Operation};

pub struct LogStorage {
  core: Mutex<LogStorageCore>,
}
impl LogStorage {
  pub fn commit(&self, tx_id: usize) {
    let rx = self.core.l().commit(tx_id);
    rx.drop_one()
  }
}

struct LogStorageCore {
  buffer: Arc<LogBuffer>,
  max_buffer_size: usize,
  last_index: usize,
  last_transaction: usize,
  checkpoint_count: usize,
  checkpoint_interval: Duration,
  writer: LogWriter,
  flush_c: StoppableChannel<Vec<LogRecord>>,
  commit_c: StoppableChannel<Vec<(usize, usize)>>,
}
impl LogStorageCore {
  fn append(&mut self, tx_id: usize, page_index: usize, data: Page) {
    self.buffer.append(tx_id, page_index, data);
    if self.buffer.len() >= self.max_buffer_size {
      self.flush_c.send(self.buffer.flush());
    }
  }

  fn new_transaction(&mut self) -> usize {
    let tx_id = self.buffer.new_transaction();
    if self.buffer.len() >= self.max_buffer_size {
      self.flush_c.send(self.buffer.flush());
    }
    return tx_id;
  }

  fn commit(&mut self, tx_id: usize) -> Receiver<()> {
    let records = self.buffer.commit(tx_id);
    let mut committed = vec![];
    for record in &records {
      if let Operation::Insert(log) = &record.operation {
        committed.push((tx_id, log.page_index))
      }
    }
    self.commit_c.send(committed);
    self.flush_c.send_with_done(records)
  }
}
