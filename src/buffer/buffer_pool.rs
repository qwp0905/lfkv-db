use std::sync::Arc;

use crate::{
  disk::PageSeeker, ContextReceiver, EmptySender, Error, Page, Result, ShortenedMutex,
  StoppableChannel, ThreadPool,
};

use super::PageCache;

pub struct BufferPool {
  cache: Arc<PageCache>,
  disk: Arc<PageSeeker>,
  background: ThreadPool<Result<()>>,
  flush_c: StoppableChannel<Vec<(usize, usize, Page)>>,
}

impl BufferPool {
  fn start_flush(&self, rx: ContextReceiver<Vec<(usize, usize, Page)>>) {
    let cache = self.cache.clone();
    let disk = self.disk.clone();
    self.background.schedule(move || {
      while let Ok((v, done)) = rx.recv_all() {
        for (tx_id, i, p) in v {
          disk.write(i, p)?;
          cache.flush(tx_id, i);
        }

        disk.fsync()?;
        done.map(|tx| tx.close());
      }
      Ok(())
    })
  }

  fn start_commit(&self, rx: ContextReceiver<usize>) {
    let cache = self.cache.clone();
    self.background.schedule(move || {
      while let Ok(tx_id) = rx.recv_new() {
        cache.commit(tx_id);
      }
      Ok(())
    });
  }

  pub fn get(&self, tx_id: usize, index: usize) -> Result<Page> {
    if let Some(page) = self.cache.get(tx_id, index) {
      return Ok(page);
    }

    let page = self.disk.read(index)?;
    if page.is_empty() {
      return Err(Error::NotFound);
    }
    self.cache.insert_from_disk(tx_id, index, page.copy());
    return Ok(page);
  }

  pub fn insert(&self, tx_id: usize, index: usize, page: Page) -> Result<()> {
    self.cache.insert_dirty(tx_id, index, page);
    Ok(())
  }
}
