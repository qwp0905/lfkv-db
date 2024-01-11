use std::sync::{Arc, Mutex};

use crossbeam::channel::Receiver;
use utils::ShortenedMutex;

use crate::{
  disk::{Page, PageSeeker},
  error::{ErrorKind, Result},
  thread::{ContextReceiver, StoppableChannel, ThreadPool},
  transaction::TransactionManager,
};

mod cache;
use cache::*;
mod list;

pub struct BufferPool {
  cache: Mutex<Cache<usize, Page>>,
  disk: Arc<PageSeeker>,
  background: ThreadPool<Result<()>>,
  transactions: Arc<TransactionManager>,
  channel: StoppableChannel<Receiver<(usize, Page)>>,
}
impl BufferPool {
  fn start_background(&self, rx: ContextReceiver<Receiver<(usize, Page)>>) {
    let disk = Arc::clone(&self.disk);
    let tx = Arc::clone(&self.transactions);
    self.background.schedule(move || {
      while let Ok(entries) = rx.recv_new() {
        for (index, page) in entries {
          let lock = tx.fetch_write_lock(index);
          disk.write(index, page)?;
          drop(lock);
        }
        disk.fsync()?;
      }

      return Ok(());
    })
  }
}

impl BufferPool {
  pub fn get(&self, index: usize) -> Result<Page> {
    if let Some(page) = { self.cache.l().get(&index) } {
      return Ok(page.copy());
    };

    let page = self.disk.read(index)?;
    if page.is_empty() {
      return Err(ErrorKind::NotFound);
    }

    self.cache.l().insert(index, page.copy());
    return Ok(page);
  }

  pub fn insert(&self, index: usize, page: Page) -> Result<()> {
    if let Some(p) = { self.cache.l().get_mut(&index) } {
      *p = page;
      return Ok(());
    };

    self.cache.l().insert(index, page);
    return Ok(());
  }

  pub fn remove(&self, index: usize) {}
}
