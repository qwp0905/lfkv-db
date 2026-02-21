use std::{
  collections::BTreeSet,
  sync::{
    atomic::{AtomicUsize, Ordering},
    RwLock,
  },
};

use crate::utils::ShortenedRwLock;

pub struct VersionVisibility {
  inner: RwLock<VersionVisibilityInner>,
  last_tx_id: AtomicUsize,
}
impl VersionVisibility {
  pub fn new(aborted: BTreeSet<usize>, last_tx_id: usize) -> Self {
    Self {
      inner: RwLock::new(VersionVisibilityInner {
        active: Default::default(),
        aborted,
      }),
      last_tx_id: AtomicUsize::new(last_tx_id),
    }
  }

  pub fn remove_aborted(&self, version: &usize) {
    let mut inner = self.inner.wl();
    inner.aborted = inner.aborted.split_off(version);
  }

  pub fn is_aborted(&self, tx_id: &usize) -> bool {
    self.inner.rl().aborted.contains(tx_id)
  }
  pub fn min_active(&self) -> Option<usize> {
    let inner = self.inner.rl();
    Some(*inner.active.first()?)
  }
  pub fn min_version(&self) -> usize {
    self
      .inner
      .rl()
      .active
      .first()
      .map(|v| *v)
      .unwrap_or_else(|| self.current_version())
  }
  pub fn deactive(&self, tx_id: &usize) {
    self.inner.wl().active.remove(tx_id);
  }
  pub fn is_visible(&self, tx_id: &usize) -> bool {
    let inner = self.inner.rl();
    !inner.aborted.contains(tx_id) && !inner.active.contains(tx_id)
  }
  pub fn is_active(&self, tx_id: &usize) -> bool {
    self.inner.rl().active.contains(tx_id)
  }
  pub fn move_to_abort(&self, tx_id: usize) {
    let mut inner = self.inner.wl();
    if inner.active.remove(&tx_id) {
      inner.aborted.insert(tx_id);
    }
  }
  pub fn new_transaction(&self) -> usize {
    let tx_id = self.last_tx_id.fetch_add(1, Ordering::Release);
    self.inner.wl().active.insert(tx_id);
    tx_id
  }
  pub fn current_version(&self) -> usize {
    self.last_tx_id.load(Ordering::Acquire)
  }
}

struct VersionVisibilityInner {
  active: BTreeSet<usize>,
  aborted: BTreeSet<usize>,
}
