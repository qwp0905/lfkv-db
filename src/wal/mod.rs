mod log;
pub use log::*;

use utils::{size, ShortenedMutex};

use std::{
  path::Path,
  sync::{Arc, Mutex},
};

use crate::{
  disk::{Page, PageSeeker},
  error::Result,
  thread::{ContextReceiver, StoppableChannel, ThreadPool},
};

pub struct WAL {
  core: Mutex<WALCore>,
}
impl WAL {
  pub fn new<T>(path: T) -> Result<Self>
  where
    T: AsRef<Path>,
  {
    let seeker = PageSeeker::open(path)?;
    if seeker.len()? == 0 {
      let header = FileHeader::new(0, 0);
      seeker.write(0, header.try_into()?)?;
    }
    return Ok(Self {
      core: Mutex::new(WALCore::new(Arc::new(seeker), 2048)),
    });
  }

  pub fn append(
    &self,
    transaction_id: usize,
    op: Operation,
    page_index: usize,
    data: Page,
  ) -> Result<()> {
    let mut core = self.core.l();
    core.append(transaction_id, op, page_index, data)
  }
}

pub struct WALCore {
  seeker: Arc<PageSeeker>,
  max_file_size: usize,
  buffer: Vec<LogEntry>,
  background: ThreadPool<Result<()>>,
  max_buffer_size: usize,
  channel: StoppableChannel<Vec<LogEntry>>,
}
impl WALCore {
  fn new(seeker: Arc<PageSeeker>, max_file_size: usize) -> Self {
    let (channel, recv) = StoppableChannel::new();
    Self {
      seeker,
      max_file_size,
      buffer: Default::default(),
      background: ThreadPool::new(1, size::mb(2), "wal", None),
      max_buffer_size: 0,
      channel,
    }
  }

  fn start_background(&self, recv: ContextReceiver<Vec<LogEntry>>) {
    let seeker = Arc::clone(&self.seeker);
    let max_file_size = self.max_file_size;
    self.background.schedule(move || {
      while let Ok(entries) = recv.recv_new() {
        for entry in entries {
          let index = entry.get_index();
          let (entry_header, entry_data) = entry.try_into()?;
          let wi = ((index * 2) % max_file_size) + 1;
          seeker.write(wi, entry_header)?;
          seeker.write(wi + 1, entry_data)?;
        }
        seeker.fsync()?;
      }
      return Ok(());
    })
  }

  fn append(
    &mut self,
    transaction_id: usize,
    op: Operation,
    page_index: usize,
    data: Page,
  ) -> Result<()> {
    let mut header: FileHeader = self.seeker.read(HEADER_INDEX)?.try_into()?;
    let i = header.last_index + 1;
    let entry = LogEntry::new(i, transaction_id, page_index, op, data);
    // let (entry_header, entry_data) = entry.try_into()?;
    self.buffer.push(entry);
    if self.buffer.len() >= self.max_buffer_size {
      self.trigger()
    }

    // let wi = ((i * 2) % self.max_file_size) + 1;
    // self.seeker.write(wi, entry_header)?;
    // self.seeker.write(wi + 1, entry_data)?;

    header.last_index = i;
    self.seeker.write(HEADER_INDEX, header.try_into()?)?;
    return self.seeker.fsync();
  }

  fn trigger(&mut self) {
    self.channel.send(self.buffer.drain(..).collect());
  }
}
