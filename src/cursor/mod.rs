use std::sync::Arc;

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
  locks: Vec<PageLock>,
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
      self.locks.push(l);

      let page = self.buffer.get(index)?;
      let entry = CursorEntry::from(index, page)?;
      match entry.find_next(&key) {
        Ok(i) => {
          let l = self.transactions.fetch_read_lock(i);
          self.locks.push(l);
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

  pub fn insert<T>(&mut self, key: String, value: T) -> Result<()>
  where
    T: TryInto<Page>,
  {
    let header = self.read_header()?;
    let mut index = header.get_root();
    let page = value.try_into().map_err(|_| ErrorKind::Unknown)?;
    loop {
      let l = self.transactions.fetch_read_lock(index);
      self.locks.push(l);

      let cp = self.buffer.get(index)?;
      let current = CursorEntry::from(index, cp)?;
      match current.find_next(&key) {
        Ok(i) => {
          let wl = self.transactions.fetch_write_lock(i);
          self.locks.push(wl);
          return self.buffer.insert(index, page);
        }
        Err(c) => match c {
          Some(i) => {
            index = i;
          }
          None => {}
        },
      }
    }
    // Ok(())
  }
}

impl Cursor {
  fn read_header(&mut self) -> Result<TreeHeader> {
    let tx = self.transactions.fetch_read_lock(0);
    self.locks.push(tx);
    let page = self.buffer.get(0)?;
    return page.try_into();
  }

  fn insert_at(
    &mut self,
    header: &mut TreeHeader,
    index: usize,
    key: String,
    page: Page,
  ) -> Result<()> {
    let l = self.transactions.fetch_read_lock(index);
    self.locks.push(l);

    let cp = self.buffer.get(index)?;
    let mut current = CursorEntry::from(index, cp)?;
    match current.find_next(&key) {
      Ok(i) => {
        let wl = self.transactions.fetch_write_lock(i);
        self.locks.push(wl);
        return self.buffer.insert(index, page);
      }
      Err(c) => match c {
        Some(i) => {}
        None => {
          let ci = header.acquire_index();
          current.add(key, ci);
        }
      },
    };
    Ok(())
  }

  fn upgrade(&mut self) {
    let drained = self.locks.drain(..);
    for lock in drained {
      let index = lock.index;
      drop(lock);
      let wl = self.transactions.fetch_write_lock(index);
      self.locks.push(wl);
    }
  }
}
