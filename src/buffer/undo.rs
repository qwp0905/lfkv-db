use crate::{disk::PageSeeker, Page};

pub struct UndoLog {
  log_index: usize,
  tx_id: usize,
  data: Page,
  undo_index: usize,
}

pub struct Rollback {
  disk: PageSeeker,
}
impl Rollback {
  pub fn get(&self, log_index: usize) {}
}
