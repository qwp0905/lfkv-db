mod log;
use crossbeam::channel::Receiver;
pub use log::*;

use utils::ShortenedMutex;

use std::{
  path::Path,
  sync::{Arc, Mutex},
};

use crate::{
  disk::{Page, PageSeeker},
  error::Result,
  thread::StoppableChannel,
};

pub struct WAL {
  core: Mutex<WALCore>,
}
impl WAL {
  pub fn new<T>(
    path: T,
    channel: StoppableChannel<Receiver<(usize, Page)>>,
  ) -> Result<Self>
  where
    T: AsRef<Path>,
  {
    let seeker = PageSeeker::open(path)?;
    if seeker.len()? == 0 {
      let header = FileHeader::new(0, 0, 0);
      seeker.write(0, header.into())?;
    }
    return Ok(Self {
      core: Mutex::new(WALCore::new(Arc::new(seeker), 2048, channel)),
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
  channel: StoppableChannel<Receiver<(usize, Page)>>,
}
impl WALCore {
  fn new(
    seeker: Arc<PageSeeker>,
    max_file_size: usize,
    channel: StoppableChannel<Receiver<(usize, Page)>>,
  ) -> Self {
    Self {
      seeker,
      max_file_size,
      channel,
    }
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
    let (entry_header, entry_data) = entry.into();

    let wi = ((i * 2) % self.max_file_size) + 1;
    self.seeker.write(wi, entry_header)?;
    self.seeker.write(wi + 1, entry_data)?;

    header.last_index = i;
    self.seeker.write(HEADER_INDEX, header.into())?;
    return Ok(());
  }
}
