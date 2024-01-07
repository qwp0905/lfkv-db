use std::sync::Arc;

use crate::{
  buffer::BufferPool, error::Result, filesystem::Page,
  transaction::TransactionManager, wal::WAL,
};

mod node;

pub struct Cursor {
  buffer: Arc<BufferPool>,
  transactions: Arc<TransactionManager>,
  wal: Arc<WAL>,
}
impl Cursor {
  // pub fn get<T>(&self, key: String) -> Result<T>
  // where
  //   T: TryFrom<Page>,
  // {
  //   if let Ok(page) = self.buffer.get(index) {}
  // }
}
