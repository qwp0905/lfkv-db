use std::sync::{Arc, Mutex};

use crossbeam::channel::{Receiver, Sender};

use crate::{
  filesystem::{Page, PageSeeker},
  thread::ThreadPool,
};

use self::cache::Cache;

mod cache;
mod list;

pub struct BufferPool {
  cache: Mutex<Cache<usize, Page>>,
  disk: Arc<PageSeeker>,
  background: ThreadPool<std::io::Result<()>>,
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
