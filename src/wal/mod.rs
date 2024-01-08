mod log;
pub use log::*;
use utils::ShortRwLocker;

use std::sync::{Arc, RwLock};

use crate::{
  disk::{Page, PageSeeker},
  error::Result,
};

pub struct WAL {
  core: RwLock<WALCore>,
}
impl WAL {
  fn append(
    &self,
    transaction_id: usize,
    op: Operation,
    page_index: usize,
    data: Page,
  ) -> Result<()> {
    let mut core = self.core.wl();
    core.append(transaction_id, op, page_index, data)
  }
}

pub struct WALCore {
  seeker: Arc<PageSeeker>,
  max_file_size: usize,
}
impl WALCore {
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

  fn new_transaction(&self) {}
}
