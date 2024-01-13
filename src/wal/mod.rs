mod log;
pub use log::*;

use utils::{size, DroppableReceiver, ShortenedMutex, ShortenedRwLock};

use std::{
  path::Path,
  sync::{Arc, Mutex, RwLock},
  time::Duration,
};

use crate::{
  disk::{Page, PageSeeker, Serializable},
  error::Result,
  thread::{ContextReceiver, StoppableChannel, ThreadPool},
};

pub struct WAL {
  core: Mutex<WALCore>,
}
impl WAL {
  pub fn new<T>(
    path: T,
    flush_c: StoppableChannel<Vec<(usize, Page)>>,
    checkpoint_timeout: Duration,
    max_log_size: usize,
    max_buffer_size: usize,
  ) -> Result<Self>
  where
    T: AsRef<Path>,
  {
    let seeker = PageSeeker::open(path)?;
    if seeker.len()? == 0 {
      let header = FileHeader::new(0, 0, 0);
      seeker.write(0, header.serialize()?)?;
    }
    return Ok(Self {
      core: Mutex::new(WALCore::new(
        Arc::new(seeker),
        max_log_size,
        flush_c,
        checkpoint_timeout,
        max_buffer_size,
      )),
    });
  }

  pub fn append(
    &self,
    transaction_id: usize,
    page_index: usize,
    data: Page,
  ) -> Result<()> {
    let mut core = self.core.l();
    core.append(transaction_id, page_index, data)
  }

  pub fn next_transaction(&self) -> Result<usize> {
    let core = self.core.l();
    core.next_transaction()
  }
}

pub struct WALCore {
  seeker: Arc<PageSeeker>,
  max_file_size: usize,
  buffer: Arc<RwLock<Vec<LogEntry>>>,
  background: ThreadPool<Result<()>>,
  max_buffer_size: usize,
  checkpoint_c: StoppableChannel<()>,
  flush_c: StoppableChannel<Vec<(usize, Page)>>,
}
impl WALCore {
  fn new(
    seeker: Arc<PageSeeker>,
    max_file_size: usize,
    flush_c: StoppableChannel<Vec<(usize, Page)>>,
    checkpoint_timeout: Duration,
    max_buffer_size: usize,
  ) -> Self {
    let (checkpoint_c, recv) = StoppableChannel::new();
    let core = Self {
      seeker,
      max_file_size,
      buffer: Default::default(),
      background: ThreadPool::new(1, size::mb(2), "wal", None),
      max_buffer_size,
      checkpoint_c,
      flush_c,
    };
    core.start_background(recv, checkpoint_timeout);
    return core;
  }

  fn start_background(&self, recv: ContextReceiver<()>, timeout: Duration) {
    let seeker = self.seeker.clone();
    let flush_c = self.flush_c.clone();
    let buffer = self.buffer.clone();
    self.background.schedule(move || {
      while let Ok(_) = recv.recv_new_or_timeout(timeout) {
        let entries: Vec<LogEntry> = { buffer.wl().drain(..).collect() };
        let to_be_applied = match entries.last() {
          Some(e) => e.get_index(),
          None => continue,
        };
        let entries = entries
          .into_iter()
          .map(|entry| (entry.get_index(), entry.into()))
          .collect();

        flush_c.send_with_done(entries).drop_one();
        let mut header: FileHeader =
          seeker.read(HEADER_INDEX)?.deserialize()?;
        header.applied = to_be_applied;
        seeker.write(HEADER_INDEX, header.serialize()?)?;
        seeker.fsync()?;
      }

      return Ok(());
    })
  }

  fn append(
    &mut self,
    transaction_id: usize,
    page_index: usize,
    data: Page,
  ) -> Result<()> {
    let mut header: FileHeader =
      self.seeker.read(HEADER_INDEX)?.deserialize()?;

    let current = header.next_index;
    let entry = LogEntry::new(current, transaction_id, page_index, data);
    {
      self.buffer.wl().push(entry.clone())
    };
    let (entry_header, entry_data) = entry.try_into()?;

    let wi = ((current * 2) % self.max_file_size) + 1;
    self.seeker.write(wi, entry_header)?;
    self.seeker.write(wi + 1, entry_data)?;

    header.next_index += 1;
    self.seeker.write(HEADER_INDEX, header.serialize()?)?;
    self.seeker.fsync()?;
    {
      if self.buffer.rl().len() >= self.max_buffer_size {
        self.checkpoint_c.send(());
      }
    };
    return Ok(());
  }

  fn next_transaction(&self) -> Result<usize> {
    let mut header: FileHeader =
      self.seeker.read(HEADER_INDEX)?.deserialize()?;
    let next = header.next_transaction;
    header.next_transaction += 1;
    self.seeker.write(HEADER_INDEX, header.serialize()?)?;
    self.seeker.fsync()?;
    return Ok(next);
  }
}
