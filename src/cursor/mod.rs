use std::{collections::HashMap, sync::Arc};

use crate::{
  buffer::BufferPool,
  error::Result,
  transaction::{PageLock, TransactionManager},
  wal::WAL,
};

use self::header::TreeHeader;

mod header;
mod node;

pub struct Cursor {
  buffer: Arc<BufferPool>,
  transactions: Arc<TransactionManager>,
  wal: Arc<WAL>,
  locks: HashMap<usize, PageLock>,
}
// impl Cursor {
//   pub fn get<T>(&self, key: String) -> Result<T>
//   where
//     T: TryFrom<Page>,
//   {
//     if let Ok(page) = self.buffer.get(index) {}
//   }
// }

impl Cursor {
  fn get_header(&mut self) -> Result<TreeHeader> {
    let tx = self.transactions.fetch_read(0);
    self.locks.insert(0, tx);
    let page = self.buffer.get(0)?;
    return page.try_into();
  }
}
