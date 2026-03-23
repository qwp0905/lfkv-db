use std::sync::atomic::{AtomicU8, Ordering};

const STATUS_AVAILABLE: u8 = 0;
const STATUS_ON_COMMIT: u8 = 1; // Exclusive state prior to commit
const STATUS_COMMITTED: u8 = 2; // The commit log has been successfully written.
const STATUS_ABORTED: u8 = 3;

pub struct TxState {
  id: usize,
  /**
   *  0: available / 1. on commit / 2. committed / 3. aborted
   */
  status: AtomicU8,
}
impl TxState {
  pub fn new(id: usize) -> Self {
    Self {
      id,
      status: AtomicU8::new(0),
    }
  }

  #[inline(always)]
  pub fn get_id(&self) -> usize {
    self.id
  }

  #[inline]
  pub fn is_available(&self) -> bool {
    self.status.load(Ordering::Acquire) == STATUS_AVAILABLE
  }

  pub fn try_abort(&self) -> bool {
    self
      .status
      .compare_exchange(
        STATUS_AVAILABLE,
        STATUS_ABORTED,
        Ordering::Release,
        Ordering::Acquire,
      )
      .is_ok()
  }

  pub fn try_commit(&self) -> bool {
    self
      .status
      .compare_exchange(
        STATUS_AVAILABLE,
        STATUS_ON_COMMIT,
        Ordering::Release,
        Ordering::Acquire,
      )
      .is_ok()
  }

  pub fn complete_commit(&self) {
    self.status.store(STATUS_COMMITTED, Ordering::Release)
  }
  pub fn make_available(&self) {
    self.status.store(STATUS_AVAILABLE, Ordering::Release)
  }
}
