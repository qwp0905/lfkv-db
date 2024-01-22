use std::collections::BTreeMap;

use crate::{second_of_two, Page};

pub trait Measurable {
  fn len(&self) -> usize;
}

pub struct MVCC {
  committed: BTreeMap<usize, Page>,
  uncommitted: BTreeMap<usize, Page>,
}
impl MVCC {
  pub fn from_committed(committed: Vec<(usize, Page)>) -> Self {
    Self {
      committed: committed.into_iter().collect(),
      uncommitted: BTreeMap::new(),
    }
  }

  pub fn from_uncommitted(uncommitted: Vec<(usize, Page)>) -> Self {
    Self {
      committed: BTreeMap::new(),
      uncommitted: uncommitted.into_iter().collect(),
    }
  }

  pub fn view(&self, tx_id: usize) -> Option<&Page> {
    self.committed.range(..tx_id).last().map(second_of_two)
  }

  pub fn append(&mut self, tx_id: usize, page: Page) {
    self.committed.insert(tx_id, page);
  }

  pub fn split_off(&mut self, tx_id: usize) {
    self.committed = self.committed.split_off(&tx_id);
  }

  pub fn is_empty(&self) -> bool {
    self.committed.len() == 0
  }

  pub fn commit(&mut self, tx_id: usize) {
    self
      .uncommitted
      .remove(&tx_id)
      .map(|p| self.committed.insert(tx_id, p));
  }
}

impl Measurable for MVCC {
  fn len(&self) -> usize {
    self.committed.len()
  }
}

impl Default for MVCC {
  fn default() -> Self {
    Self {
      committed: Default::default(),
      uncommitted: Default::default(),
    }
  }
}
