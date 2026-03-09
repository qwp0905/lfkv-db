use std::{
  collections::BTreeSet,
  panic::RefUnwindSafe,
  sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_skiplist::SkipSet;

pub struct VersionVisibility {
  active: SkipSet<usize>,
  aborted: SkipSet<usize>,
  last_tx_id: AtomicUsize,
}
impl VersionVisibility {
  pub fn new(aborted: BTreeSet<usize>, last_tx_id: usize) -> Self {
    Self {
      active: Default::default(),
      aborted: SkipSet::from_iter(aborted),
      last_tx_id: AtomicUsize::new(last_tx_id),
    }
  }
  pub fn current_version(&self) -> usize {
    self.last_tx_id.load(Ordering::Acquire)
  }
  pub fn min_version(&self) -> usize {
    self
      .active
      .front()
      .map(|v| *v)
      .unwrap_or_else(|| self.current_version())
  }
  pub fn is_visible(&self, tx_id: &usize) -> bool {
    !self.is_active(tx_id) && !self.is_aborted(tx_id)
  }
  pub fn remove_aborted(&self, &version: &usize) {
    while let Some(entry) = self.aborted.front() {
      if *entry.value() >= version {
        break;
      }
      entry.remove();
    }
  }

  pub fn is_active(&self, tx_id: &usize) -> bool {
    self.active.contains(tx_id)
  }
  pub fn is_aborted(&self, tx_id: &usize) -> bool {
    self.aborted.contains(tx_id)
  }

  pub fn deactive(&self, tx_id: &usize) {
    self.active.remove(tx_id);
  }

  pub fn move_to_abort(&self, tx_id: usize) {
    self.deactive(self.aborted.insert(tx_id).value());
  }
  pub fn new_transaction(&self) -> usize {
    *self
      .active
      .insert(self.last_tx_id.fetch_add(1, Ordering::Release))
      .value()
  }
}

unsafe impl Send for VersionVisibility {}
unsafe impl Sync for VersionVisibility {}
impl RefUnwindSafe for VersionVisibility {}
