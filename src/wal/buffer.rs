use std::{collections::BTreeMap, sync::Mutex};

use crate::{replace_default, Page, ShortenedMutex};

use super::LogRecord;

struct LogBufferCore {
  last_transaction: usize,
  map: BTreeMap<usize, Vec<LogRecord>>,
  size: usize,
}
pub struct LogBuffer(Mutex<LogBufferCore>);

impl LogBuffer {
  pub fn new_transaction(&self) -> usize {
    let mut core = self.0.l();
    let tx_id = core.last_transaction + 1;
    core.map.insert(tx_id, vec![LogRecord::new_start(tx_id)]);
    core.last_transaction = tx_id;
    core.size += 1;
    return tx_id;
  }

  pub fn append(&self, tx_id: usize, page_index: usize, data: Page) {
    let mut core = self.0.l();
    let record = LogRecord::new_insert(tx_id, page_index, data);
    core.map.entry(tx_id).or_default().push(record);
    core.size += 1;
  }

  pub fn commit(&self, tx_id: usize) -> Vec<LogRecord> {
    let mut core = self.0.l();
    let mut records = core.map.remove(&tx_id).unwrap_or_default();
    core.size -= records.len();
    records.push(LogRecord::new_commit(tx_id));
    return records;
  }

  pub fn len(&self) -> usize {
    self.0.l().size
  }

  pub fn flush(&self) -> Vec<LogRecord> {
    let mut core = self.0.l();
    core.size = 0;
    replace_default(&mut core.map)
      .into_values()
      .flatten()
      .collect()
  }
}
