use crate::{disk::PageSeeker, size, Result, Serializable};

use super::{Record, RecordEntry};

pub const WAL_PAGE_SIZE: usize = size::kb(32);

pub struct RotateWriter {
  entries: Vec<RecordEntry>,
  cursor: usize,
  disk: PageSeeker<WAL_PAGE_SIZE>,
  max_file_size: usize,
}
impl RotateWriter {
  pub fn open() {}

  pub fn append(&mut self, record: Record) -> Result<()> {
    let current = match self.entries.last_mut() {
      Some(entry) if entry.is_available(&record) => entry,
      _ => {
        self.cursor = (self.cursor + 1) % self.max_file_size;
        self.entries.push(RecordEntry::new());
        self.entries.last_mut().unwrap()
      }
    };

    current.append(record);
    self.disk.write(self.cursor, current.serialize()?)?;
    self.disk.fsync()
  }

  pub fn drain_buffer(&mut self) -> Vec<RecordEntry> {
    std::mem::replace(&mut self.entries, vec![])
  }
}
