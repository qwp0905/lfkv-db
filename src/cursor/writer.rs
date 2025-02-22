use std::sync::Arc;

use crate::{
  buffer::{BufferPool, BLOCK_SIZE},
  disk::FreeList,
  wal::WriteAheadLog,
  Page, Result,
};

pub struct CursorWriter {
  tx_id: usize,
  last_commit_index: usize,
  wal: Arc<WriteAheadLog>,
  buffer: Arc<BufferPool>,
  freelist: Arc<FreeList<BLOCK_SIZE>>,
}
impl CursorWriter {
  pub fn new(
    tx_id: usize,
    last_commit_index: usize,
    wal: Arc<WriteAheadLog>,
    buffer: Arc<BufferPool>,
    freelist: Arc<FreeList<BLOCK_SIZE>>,
  ) -> Self {
    Self {
      tx_id,
      last_commit_index,
      wal,
      buffer,
      freelist,
    }
  }

  pub fn get_id(&self) -> usize {
    self.tx_id
  }

  pub fn get(&self, index: usize) -> Result<Page> {
    self.buffer.get(self.last_commit_index, index)
  }

  pub fn update(&self, index: usize, page: Page) -> Result {
    self.buffer.insert(self.tx_id, index, page.copy())?;
    self.wal.append(self.tx_id, index, page)
  }

  pub fn insert(&self, page: Page) -> Result<usize> {
    let index = self.freelist.acquire();
    self.buffer.insert(self.tx_id, index, page.copy())?;
    self.wal.append(self.tx_id, index, page)?;
    Ok(index)
  }

  pub fn commit(&self) -> Result {
    self.wal.commit(self.tx_id)
  }

  pub fn release(&self, index: usize) -> Result {
    unimplemented!()
  }
}
