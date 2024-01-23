use std::{
  mem::replace,
  sync::Arc,
  time::{Duration, Instant},
};

use crate::{
  disk::PageSeeker, ContextReceiver, EmptySender, Page, Result, Serializable,
  StoppableChannel, ThreadPool,
};

use super::{LogEntry, LogRecord, WAL_PAGE_SIZE};

pub struct LogWriter {
  disk: Arc<PageSeeker<WAL_PAGE_SIZE>>,
  current: LogEntry,
  max_file_size: usize,
  cursor: usize,
  background: ThreadPool<Result<()>>,
  max_group_commit_delay: Duration,
  max_group_commit_count: usize,
  write_c: StoppableChannel<(usize, Page<WAL_PAGE_SIZE>)>,
}

impl LogWriter {
  fn start_write_c(&self, rx: ContextReceiver<(usize, Page<WAL_PAGE_SIZE>)>) {
    let delay = self.max_group_commit_delay;
    let count = self.max_group_commit_count;
    let disk = self.disk.clone();
    self.background.schedule(move || {
      let mut v = vec![];
      let mut start = Instant::now();
      let mut point = delay;
      while let Ok(o) = rx.recv_done_or_timeout(point) {
        if let Some(((index, page), done)) = o {
          disk.write(index, page)?;
          v.push(done);
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

  pub fn append(&mut self, record: LogRecord) -> Result<()> {
    if self.current.is_available(&record) {
      self.current.records.push(record);
    } else {
      let replaced = replace(&mut self.current, LogEntry::new(vec![record]));
      self.disk.write(self.cursor, replaced.serialize()?)?;
      self.cursor += 1;
    }

    Ok(())
  }

  pub fn batch_write() {}
}
