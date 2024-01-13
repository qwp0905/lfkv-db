use std::sync::Arc;

use crate::transaction::{LockManager, PageLock};

pub struct CursorLocks {
  manager: Arc<LockManager>,
  acquired: Vec<PageLock>,
}
impl CursorLocks {
  pub fn new(manager: Arc<LockManager>) -> Self {
    Self {
      manager,
      acquired: Default::default(),
    }
  }

  pub fn fetch_read(&mut self, index: usize) {
    let lock = self.manager.fetch_read_lock(index);
    self.acquired.push(lock)
  }

  pub fn fetch_write(&mut self, index: usize) {
    let lock = self.manager.fetch_write_lock(index);
    self.acquired.push(lock)
  }

  pub fn release_all(&mut self) {
    self.acquired.drain(..);
  }

  // pub fn pop(&mut self) {
  //   self.acquired.pop();
  // }
}
