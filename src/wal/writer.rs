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
  pub fn new(disk: PageSeeker<WAL_PAGE_SIZE>, max_file_size: usize) -> Self {
    Self {
      entries: vec![],
      cursor: 0,
      disk,
      max_file_size,
    }
  }

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

  pub fn buffered_count(&self) -> usize {
    self.entries.len()
  }

  pub fn read_all(&mut self) -> Result<Vec<RecordEntry>> {
    let mut v = vec![];
    let mut r = vec![];
    if self.disk.len()? == 0 {
      return Ok(v);
    }
    let mut last = 0;
    for i in 0..self.max_file_size {
      let page = match self.disk.read(i) {
        Ok(page) => page,
        Err(_) => break,
      };

      let entry: RecordEntry = page.deserialize()?;
      r.push(entry.clone());
      let first_index = match entry.first() {
        Some(record) => record.index,
        None => continue,
      };

      if first_index < last {
        v = std::mem::replace(&mut r, vec![]);
        self.cursor = i;
        self.entries.push(entry);
      }
      last = first_index;
    }

    v.append(&mut r);
    return Ok(v);
  }
}
