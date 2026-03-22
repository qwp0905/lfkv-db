use std::{
  panic::RefUnwindSafe,
  sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_skiplist::SkipSet;

pub struct VersionVisibility {
  aborted: SkipSet<usize>,
  active: SkipSet<usize>,
  last_tx_id: AtomicUsize,
}
impl VersionVisibility {
  pub fn new<T>(aborted: T, last_tx_id: usize) -> Self
  where
    T: IntoIterator<Item = usize>,
  {
    Self {
      active: Default::default(),
      aborted: SkipSet::from_iter(aborted),
      last_tx_id: AtomicUsize::new(last_tx_id),
    }
  }

  pub fn remove_aborted(&self, version: &usize) {
    while let Some(v) = self.aborted.front() {
      if v.value() >= version {
        return;
      }
      v.remove();
    }
  }

  pub fn is_aborted(&self, tx_id: &usize) -> bool {
    self.aborted.contains(tx_id)
  }
  pub fn min_version(&self) -> usize {
    self
      .active
      .front()
      .map(|v| *v.value())
      .unwrap_or_else(|| self.current_version())
  }
  pub fn deactive(&self, tx_id: &usize) {
    self.active.remove(tx_id);
  }
  pub fn is_visible(&self, tx_id: &usize) -> bool {
    !self.aborted.contains(tx_id) && !self.active.contains(tx_id)
  }
  pub fn is_active(&self, tx_id: &usize) -> bool {
    self.active.contains(tx_id)
  }
  pub fn move_to_abort(&self, tx_id: usize) {
    self.active.remove(self.aborted.insert(tx_id).value());
  }
  pub fn new_transaction(&self) -> usize {
    let tx_id = self.last_tx_id.fetch_add(1, Ordering::Release);
    *self.active.insert(tx_id).value()
  }
  pub fn current_version(&self) -> usize {
    self.last_tx_id.load(Ordering::Acquire)
  }
}
impl RefUnwindSafe for VersionVisibility {}
