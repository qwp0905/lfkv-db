use std::{
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use crossbeam::channel::Receiver;

use crate::{
  disk::PageSeeker, ContextReceiver, DroppableReceiver, EmptySender, Page, Result,
  ShortenedMutex, StoppableChannel, ThreadPool,
};

use super::{LogBuffer, LogWriter, WAL_PAGE_SIZE};

pub struct LogStorage {
  core: Mutex<LogStorageCore>,
}
impl LogStorage {
  pub fn new_transaction(&self) -> Result<usize> {
    self.core.l().new_transaction()
  }

  pub fn append(&self, tx_id: usize, page_index: usize, data: Page) -> Result<()> {
    self.core.l().append(tx_id, page_index, data)
  }

  pub fn commit(&self, tx_id: usize) -> Result<()> {
    let rx = self.core.l().commit(tx_id)?;
    Ok(rx.drop_one())
  }
}

struct LogStorageCore {
  buffer: Arc<LogBuffer>,
  max_buffer_size: usize,
  last_transaction: usize,
  checkpoint_interval: Duration,
  writer: LogWriter,
  commit_c: StoppableChannel<usize>,
  background: ThreadPool<Result<()>>,
  disk: Arc<PageSeeker<WAL_PAGE_SIZE>>,
  max_group_commit_delay: Duration,
  max_group_commit_count: usize,
}
impl LogStorageCore {
  fn start_write(&self, rx: ContextReceiver<Vec<(usize, Page<WAL_PAGE_SIZE>)>>) {
    let delay = self.max_group_commit_delay;
    let count = self.max_group_commit_count;
    let disk = self.disk.clone();
    self.background.schedule(move || {
      let mut v = vec![];
      let mut start = Instant::now();
      let mut point = delay;
      while let Ok(o) = rx.recv_done_or_timeout(point) {
        if let Some((pages, done)) = o {
          v.push(done);
          for (index, page) in pages {
            disk.write(index, page)?;
          }
          if v.len() < count {
            point -= Instant::now().duration_since(start).min(point);
            start = Instant::now();
            continue;
          }
        }
        disk.fsync()?;
        v.drain(..).for_each(|done| done.close());
        point = delay;
        start = Instant::now();
      }
      Ok(())
    });
  }

  fn start_checkpoint(&self, rx: ContextReceiver<()>) {
    let timeout = self.checkpoint_interval;
    self.background.schedule(move || {
      while let Ok(_) = rx.recv_new_or_timeout(timeout) {}
      return Ok(());
    });
  }

  fn append(&mut self, tx_id: usize, page_index: usize, data: Page) -> Result<()> {
    self.buffer.append(tx_id, page_index, data);
    if self.buffer.len() >= self.max_buffer_size {
      self.writer.batch_write(self.buffer.flush())?;
    }
    Ok(())
  }

  fn new_transaction(&mut self) -> Result<usize> {
    let tx_id = self.buffer.new_transaction();
    if self.buffer.len() >= self.max_buffer_size {
      self.writer.batch_write(self.buffer.flush())?;
    }
    return Ok(tx_id);
  }

  fn commit(&mut self, tx_id: usize) -> Result<Receiver<()>> {
    let records = self.buffer.commit(tx_id);
    self.commit_c.send(tx_id);
    self.writer.batch_write(records)
  }
}
