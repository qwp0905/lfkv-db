use std::{
  collections::BTreeMap,
  ops::{Add, AddAssign, SubAssign},
  sync::Mutex,
};

use crate::{Drain, Page, ShortenedMutex};

use super::LogRecord;

struct LogBufferCore {
  last_transaction: usize,
  map: BTreeMap<usize, Vec<LogRecord>>,
  size: usize,
}
pub struct LogBuffer(Mutex<LogBufferCore>);

impl LogBuffer {
  pub fn new() -> Self {
    Self(Mutex::new(LogBufferCore {
      last_transaction: 0,
      map: Default::default(),
      size: 0,
    }))
  }

  pub fn initial_state(&self, last_transaction: usize) {
    let mut core = self.0.l();
    core.last_transaction = last_transaction
  }

  pub fn new_transaction(&self) -> usize {
    let mut core = self.0.l();
    let tx_id = core.last_transaction.add(1);
    let record = LogRecord::new_start(tx_id);
    core.size.add_assign(record.size());
    core.map.insert(tx_id, vec![LogRecord::new_start(tx_id)]);
    core.last_transaction = tx_id;
    tx_id
  }

  pub fn append(&self, tx_id: usize, page_index: usize, data: Page) {
    let mut core = self.0.l();
    let record = LogRecord::new_insert(tx_id, page_index, data);
    core.size.add_assign(record.size());
    core.map.entry(tx_id).or_default().push(record);
  }

  pub fn commit(&self, tx_id: usize) -> Vec<LogRecord> {
    let mut core = self.0.l();
    let mut records = core.map.remove(&tx_id).unwrap_or_default();
    core
      .size
      .sub_assign(records.iter().fold(0, |a, r| a.add(r.size())));
    records.push(LogRecord::new_commit(tx_id));
    records
  }

  pub fn rollback(&self, tx_id: usize) {
    let mut core = self.0.l();
    core.map.remove(&tx_id).map(|records| {
      core
        .size
        .sub_assign(records.iter().fold(0, |a, r| a.add(r.size())))
    });
  }

  pub fn len(&self) -> usize {
    self.0.l().size
  }

  pub fn flush(&self) -> Vec<LogRecord> {
    let mut core = self.0.l();
    core.size = 0;
    core.map.drain().into_values().flatten().collect()
  }
}
