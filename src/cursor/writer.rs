use std::sync::Arc;

use crate::{
  buffer::BufferPool, wal::WriteAheadLog, Error, Result, Serializable, PAGE_SIZE,
};

pub struct CursorWriter {
  tx_id: usize,
  last_commit_index: usize,
  wal: Arc<WriteAheadLog>,
  buffer: Arc<BufferPool>,
}
impl CursorWriter {
  pub fn new(
    tx_id: usize,
    last_commit_index: usize,
    wal: Arc<WriteAheadLog>,
    buffer: Arc<BufferPool>,
  ) -> Self {
    Self {
      tx_id,
      last_commit_index,
      wal,
      buffer,
    }
  }

  pub fn get<T>(&self, index: usize) -> Result<T>
  where
    T: Serializable<Error, PAGE_SIZE>,
  {
    let page = self.buffer.get(self.last_commit_index, index)?;
    page.deserialize()
  }

  pub fn insert<T>(&self, index: usize, value: T) -> Result
  where
    T: Serializable<Error, PAGE_SIZE>,
  {
    let page = value.serialize()?;
    self.buffer.insert(self.tx_id, index, page.copy())?;
    self.wal.append(self.tx_id, index, page)
  }

  pub fn commit(&self) -> Result {
    self.wal.commit(self.tx_id)
  }
}
