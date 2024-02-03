use std::collections::BTreeMap;

use crate::{second_of_two, wal::CommitInfo, Page, Serializable};

pub trait Measurable {
  fn len(&self) -> usize;
}

pub struct MVCC {
  committed: BTreeMap<usize, Page>,
  uncommitted: BTreeMap<usize, Page>,
}
impl MVCC {
  pub fn view(&self, commit: CommitInfo) -> Option<&Page> {
    if let Some(page) = self.uncommitted.get(&commit.tx_id) {
      return Some(page);
    };

    self
      .committed
      .range(..commit.commit_index)
      .last()
      .map(second_of_two)
  }

  pub fn commit(&mut self, commit: CommitInfo) {
    self.uncommitted.remove(&commit.tx_id).map(|page| {
      self.committed.insert(commit.commit_index, page);
    });
  }

  pub fn vacuum(&mut self, log_index: usize) {
    self.committed = self.committed.split_off(&log_index)
  }

  pub fn append_uncommitted(&mut self, tx_id: usize, page: Page) {
    self.uncommitted.insert(tx_id, page);
  }
}

// impl Serializable for MVCC {
//   fn serialize(&self) -> Result<Page, crate::Error> {}
//   fn deserialize(value: &Page) -> Result<Self, crate::Error> {}
// }
