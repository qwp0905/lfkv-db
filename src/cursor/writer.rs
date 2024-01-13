use std::sync::Arc;

use crate::{buffer::BufferPool, disk::Page, error::Result, wal::WAL};

pub struct CursorWriter {
  transaction_id: usize,
  wal: Arc<WAL>,
  buffer: Arc<BufferPool>,
}
impl CursorWriter {
  pub fn new(
    transaction_id: usize,
    wal: Arc<WAL>,
    buffer: Arc<BufferPool>,
  ) -> Self {
    Self {
      transaction_id,
      wal,
      buffer,
    }
  }

  pub fn get(&self, index: usize) -> Result<Page> {
    self.buffer.get(index)
  }

  pub fn insert(&self, index: usize, page: Page) -> Result<()> {
    self.wal.append(self.transaction_id, index, page.copy())?;
    self.buffer.insert(index, page)
  }
}
