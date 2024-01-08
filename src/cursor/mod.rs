use std::{collections::VecDeque, sync::Arc};

use crate::{
  buffer::BufferPool,
  error::Result,
  filesystem::Page,
  transaction::{PageLock, TransactionManager},
  wal::WAL,
};

mod header;
use header::*;
mod node;
use node::*;

pub struct Cursor {
  buffer: Arc<BufferPool>,
  transactions: Arc<TransactionManager>,
  wal: Arc<WAL>,
  locks: VecDeque<PageLock>,
}
impl Cursor {
  pub fn get<T>(&mut self, key: String) -> Result<T>
  where
    T: TryFrom<Page>,
  {
    let header = self.read_header()?;
    let mut index = header.get_root();
    loop {
      let l = self.transactions.fetch_read(index);
      self.locks.push_back(l);

      let page = self.buffer.get(index)?;
      // let entry =
    }
  }
}

impl Cursor {
  fn read_header(&mut self) -> Result<TreeHeader> {
    let tx = self.transactions.fetch_read(0);
    self.locks.push_back(tx);
    let page = self.buffer.get(0)?;
    return page.try_into();
  }
}
