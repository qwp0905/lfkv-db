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
    println!("{}", index);
    let before = self.buffer.get(index).unwrap_or(Page::new_empty());
    println!("{}", index);
    self
      .wal
      .append(self.transaction_id, index, before, page.copy())?;
    // self.wal.append(self.transaction_id, index, page.copy())?;
    println!("{}", index);
    self.buffer.insert(index, page)
  }

  // pub fn remove(&self, index: usize) -> Result<()> {
  //   let page = Page::new_empty();
  //   self.wal.append(self.transaction_id, index, page.copy())?;
  //   self.buffer.insert(index, page)
  // }
}

impl Drop for CursorWriter {
  fn drop(&mut self) {
    for _ in 0..1 {
      if self.wal.commit(self.transaction_id).is_ok() {
        return;
      };
    }
  }
}
