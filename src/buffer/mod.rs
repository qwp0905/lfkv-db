use std::sync::{Arc, Mutex};

use crossbeam::channel::{Receiver, Sender};
use utils::ShortLocker;

use crate::{
  disk::{Page, PageSeeker},
  error::{ErrorKind, Result},
  thread::ThreadPool,
};

use self::cache::Cache;

mod cache;
mod list;

pub struct BufferPool {
  cache: Mutex<Cache<usize, Page>>,
  disk: Arc<PageSeeker>,
  background: ThreadPool<Result<()>>,
  channel: Sender<Option<Vec<(usize, Page)>>>,
}
impl BufferPool {
  fn start_background(&self, rx: Receiver<Option<Vec<(usize, Page)>>>) {
    let disk = Arc::clone(&self.disk);
    self.background.schedule(move || {
      while let Ok(Some(entries)) = rx.recv() {
        for (index, page) in entries {
          disk.write(index, page)?;
        }
        disk.fsync()?;
      }

      return Ok(());
    })
  }
}
impl Drop for BufferPool {
  fn drop(&mut self) {
    self.channel.send(None).unwrap();
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
}
