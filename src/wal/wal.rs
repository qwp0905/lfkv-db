use std::{
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
  },
  time::{Duration, Instant},
};

use crossbeam::channel::Receiver;

use crate::{
  disk::PageSeeker, replace_default, ContextReceiver, DroppableReceiver, EmptySender,
  Page, Result, Serializable, ShortenedMutex, StoppableChannel, ThreadPool,
};

use super::{LogBuffer, LogEntry, LogRecord, WAL_PAGE_SIZE};

#[derive(Debug, Clone)]
pub struct LogStorageConfig {
  max_buffer_size: usize,
  checkpoint_interval: Duration,
  group_commit_delay: Duration,
  group_commit_count: usize,
  max_file_size: usize,
}

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
  commit_c: StoppableChannel<usize>,
  background: ThreadPool<Result<()>>,
  disk: Arc<PageSeeker<WAL_PAGE_SIZE>>,
  io_c: StoppableChannel<Vec<LogRecord>>,
  checkpoint_c: StoppableChannel<()>,
  config: LogStorageConfig,
  flush_c: StoppableChannel<Vec<(usize, usize, Page)>>,
  cursor: Arc<AtomicUsize>,
}
impl LogStorageCore {
  fn start_io(
    &self,
    rx: ContextReceiver<Vec<LogRecord>>,
    records: Vec<LogRecord>,
    mut last_index: usize,
    mut cursor: usize,
  ) {
    let delay = self.config.group_commit_delay;
    let count = self.config.group_commit_count;
    let max_file_size = self.config.max_file_size;
    let disk = self.disk.clone();
    let cursor = self.cursor.clone();
    self.background.schedule(move || {
      let mut current = LogEntry::new(records);
      let mut start = Instant::now();
      let mut point = delay;
      let mut v = vec![];
      while let Ok(o) = rx.recv_done_or_timeout(point) {
        if let Some((records, done)) = o {
          v.push(done);
          for mut record in records {
            record.index = last_index + 1;
            last_index += 1;

            if current.is_available(&record) {
              let now = cursor.get_mut();
              let entry = replace_default(&mut current);
              disk.write(*now, entry.serialize()?)?;
              *now = (*now + 1) % max_file_size;
            }
            current.append(record);
          }

          disk.write(cursor.load(Ordering::SeqCst), current.serialize()?)?;

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
      return Ok(());
    });
  }

  fn start_checkpoint(&self, rx: ContextReceiver<usize>) {
    let timeout = self.config.checkpoint_interval;
    let flush_c = self.flush_c.clone();
    let disk = self.disk.clone();
    let io_c = self.io_c.clone();
    self.background.schedule(move || {
      while let Ok(to_be_apply) = rx.recv_new_or_timeout(timeout) {
        let mut v = vec![];
        disk.read(1)?;
        let rx = flush_c.send_with_done(v);
        rx.drop_one();
        io_c
          .send_with_done(vec![LogRecord::new_checkpoint(todo!())])
          .drop_one();
      }
      return Ok(());
    });
  }

  fn append(&mut self, tx_id: usize, page_index: usize, data: Page) -> Result<()> {
    self.buffer.append(tx_id, page_index, data);
    if self.buffer.len() >= self.config.max_buffer_size {
      self.io_c.send_with_done(self.buffer.flush());
    }
    Ok(())
  }

  fn new_transaction(&mut self) -> Result<usize> {
    let tx_id = self.buffer.new_transaction();
    if self.buffer.len() >= self.config.max_buffer_size {
      self.io_c.send_with_done(self.buffer.flush());
    }
    return Ok(tx_id);
  }

  fn commit(&mut self, tx_id: usize) -> Result<Receiver<()>> {
    let records = self.buffer.commit(tx_id);
    self.commit_c.send(tx_id);
    Ok(self.io_c.send_with_done(records))
  }
}
