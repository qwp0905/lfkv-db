use std::{collections::VecDeque, sync::Arc};

use crate::{
  buffer::BufferPool,
  disk::Page,
  error::{ErrorKind, Result},
  transaction::{PageLock, TransactionManager},
  wal::WAL,
};

mod header;
use header::*;
mod node;
// use node::*;
mod entry;
use entry::*;

pub struct Cursor {
  buffer: Arc<BufferPool>,
  transactions: Arc<TransactionManager>,
  wal: Arc<WAL>,
  locks: VecDeque<PageLock>,
}
impl Cursor {
  pub fn new(
    buffer: Arc<BufferPool>,
    transactions: Arc<TransactionManager>,
    wal: Arc<WAL>,
  ) -> Self {
    Self {
      buffer,
      transactions,
      wal,
      locks: Default::default(),
    }
  }

  pub fn get<T>(&mut self, key: String) -> Result<T>
  where
    T: TryFrom<Page>,
  {
    let header = self.read_header()?;
    let mut index = header.get_root();
    loop {
      let l = self.transactions.fetch_read_lock(index);
      self.locks.push_back(l);

      let page = self.buffer.get(index)?;
      let entry = CursorEntry::from(index, page)?;
      match entry.find_next(&key) {
        Ok(i) => {
          let l = self.transactions.fetch_read_lock(i);
          self.locks.push_back(l);
          let p = self.buffer.get(i)?;
          return p.try_into().map_err(|_| ErrorKind::Unknown);
        }
        Err(c) => match c {
          None => return Err(ErrorKind::NotFound),
          Some(i) => {
            index = i;
          }
        },
      }
    }
  }
}

impl Cursor {
  fn read_header(&mut self) -> Result<TreeHeader> {
    let tx = self.transactions.fetch_read_lock(0);
    self.locks.push_back(tx);
    let page = self.buffer.get(0)?;
    return page.try_into();
  }
}
