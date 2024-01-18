use crate::{disk::Page, size, PAGE_SIZE};

pub const WAL_PAGE_SIZE: usize = size::kb(32);

#[derive(Debug)]
pub struct InsertLog {
  pub page_index: usize,
  pub before: Page,
  pub after: Page,
}
impl InsertLog {
  fn new(page_index: usize, before: Page, after: Page) -> Self {
    Self {
      page_index,
      before,
      after,
    }
  }
}
impl Clone for InsertLog {
  fn clone(&self) -> Self {
    Self {
      page_index: self.page_index,
      before: self.before.copy(),
      after: self.after.copy(),
    }
  }
}

#[derive(Debug, Clone)]
pub enum Operation {
  Start,
  Commit,
  Abort,
  Checkpoint(usize),
  Insert(InsertLog),
}
impl Operation {
  fn size(&self) -> usize {
    match self {
      Operation::Start => 1,
      Operation::Commit => 1,
      Operation::Abort => 1,
      Operation::Checkpoint(_) => 9,
      Operation::Insert(_) => 8 + PAGE_SIZE * 2,
    }
  }
}

pub struct LogRecord {
  index: usize,
  transaction_id: usize,
  operation: Operation,
}
impl LogRecord {
  pub fn new_start(index: usize, transaction_id: usize) -> Self {
    Self::new(index, transaction_id, Operation::Start)
  }

  pub fn new_commit(index: usize, transaction_id: usize) -> Self {
    Self::new(index, transaction_id, Operation::Commit)
  }

  pub fn new_abort(index: usize, transaction_id: usize) -> Self {
    Self::new(index, transaction_id, Operation::Commit)
  }

  pub fn new_insert(
    index: usize,
    transaction_id: usize,
    page_index: usize,
    before: Page,
    after: Page,
  ) -> Self {
    Self::new(
      index,
      transaction_id,
      Operation::Insert(InsertLog::new(page_index, before, after)),
    )
  }

  pub fn new_checkpoint(index: usize, applied: usize) -> Self {
    Self::new(index, 0, Operation::Checkpoint(applied))
  }

  fn new(index: usize, transaction_id: usize, operation: Operation) -> Self {
    Self {
      index,
      transaction_id,
      operation,
    }
  }

  fn size(&self) -> usize {
    self.operation.size() + 16
  }
}

pub struct LogEntry {
  pub records: Vec<LogRecord>,
}
impl LogEntry {}
