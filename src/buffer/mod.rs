use std::sync::{Arc, Mutex};

use crossbeam::channel::{Receiver, Sender};
use utils::ShortLocker;

use crate::{
  error::{ErrorKind, Result},
  filesystem::{Page, PageSeeker},
  thread::ThreadPool,
};

use self::cache::Cache;

mod cache;
mod list;

pub struct BufferPool {
  cache: Mutex<Cache<usize, Page>>,
  disk: Arc<PageSeeker>,
  background: ThreadPool<Result<()>>,
  channel: Sender<Option<(usize, Page)>>,
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

    if let Some((ei, evicted)) = { self.cache.l().insert(index, page.copy()) } {
      self.channel.send(Some((ei, evicted))).unwrap();
    }
    return Ok(page);
  }
}
