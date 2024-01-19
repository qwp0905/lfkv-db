use std::{
  sync::{Arc, RwLock},
  time::{Duration, Instant},
};

use crossbeam::channel::Sender;

use crate::{
  disk::PageSeeker, ContextReceiver, EmptySender, Result, StoppableChannel,
  ThreadPool,
};

use super::LogRecord;

pub struct LogStorage {
  core: RwLock<LogStorageCore>,
}

struct LogStorageCore {
  buffer: Vec<LogRecord>,
  last_index: usize,
  last_transaction: usize,
  max_buffer_size: usize,
  max_group_commit_delay: Duration,
  max_group_commit_count: usize,
  background: ThreadPool<Result<()>>,
  fsync_c: StoppableChannel<Sender<()>>,
}
impl LogStorageCore {
  fn start_fsync(
    &self,
    rx: ContextReceiver<Sender<()>>,
    disk: Arc<PageSeeker>,
  ) {
    let delay = self.max_group_commit_delay;
    let count = self.max_group_commit_count;
    self.background.schedule(move || {
      let mut v = vec![];
      let mut start = Instant::now();
      let mut point = delay;
      while let Ok(o) = rx.recv_new_or_timeout(point) {
        point = point
          .checked_sub(Instant::now().duration_since(start))
          .unwrap_or(Duration::new(0, 0));
        start = Instant::now();

        if let Some(tx) = o {
          v.push(tx);
          if v.len() < count {
            continue;
          }
        }

        disk.fsync()?;
        v.drain(..).for_each(|tx| tx.close());
        point = delay;
      }
      Ok(())
    });
  }

  fn append(&mut self, record: LogRecord) {
    self.buffer.push(record);
    if self.buffer.len() >= self.max_buffer_size {}
  }
}
