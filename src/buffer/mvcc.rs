use std::collections::BTreeMap;

use crate::{second_of_two, Page};

pub trait Measurable {
  fn len(&self) -> usize;
}

pub struct MVCC {
  versions: BTreeMap<usize, Page>,
}
impl MVCC {
  pub fn new(versions: Vec<(usize, Page)>) -> Self {
    Self {
      versions: versions.into_iter().collect(),
    }
  }

  pub fn view(&self, tx_id: usize) -> Option<&Page> {
    self.versions.range(..tx_id).last().map(second_of_two)
  }

  pub fn append(&mut self, tx_id: usize, page: Page) {
    self.versions.insert(tx_id, page);
  }

  pub fn split_off(&mut self, tx_id: usize) {
    self.versions = self.versions.split_off(&tx_id);
  }

  pub fn is_empty(&self) -> bool {
    self.versions.len() == 0
  }
}

impl Measurable for MVCC {
  fn len(&self) -> usize {
    self.versions.len()
  }
}

impl Default for MVCC {
  fn default() -> Self {
    Self {
      versions: Default::default(),
    }
  }
}
