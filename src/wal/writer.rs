use std::mem::replace;

use crossbeam::channel::Receiver;

use crate::{Page, Result, Serializable, StoppableChannel};

use super::{LogEntry, LogRecord, WAL_PAGE_SIZE};

pub struct LogWriter {
  current: LogEntry,
  max_file_size: usize,
  last_index: usize,
  cursor: usize,
  write_c: StoppableChannel<Vec<(usize, Page<WAL_PAGE_SIZE>)>>,
  checkpoint_c: StoppableChannel<()>,
}

impl LogWriter {
  pub fn new(
    max_file_size: usize,
    write_c: StoppableChannel<Vec<(usize, Page<WAL_PAGE_SIZE>)>>,
    checkpoint_c: StoppableChannel<()>,
  ) -> Self {
    Self {
      current: Default::default(),
      max_file_size,
      last_index: 0,
      cursor: 0,
      write_c,
      checkpoint_c,
    }
  }

  pub fn initialize(
    &mut self,
    records: Vec<LogRecord>,
    last_index: usize,
    cursor: usize,
  ) {
    records
      .into_iter()
      .for_each(|record| self.current.append(record));
    self.last_index = last_index;
    self.cursor = cursor
  }

  pub fn batch_write(&mut self, records: Vec<LogRecord>) -> Result<Receiver<()>> {
    let mut pages = vec![];
    for mut record in records {
      record.index = self.last_index + 1;
      self.last_index += 1;

      if !self.current.is_available(&record) {
        let entry = replace(&mut self.current, Default::default());
        pages.push((self.cursor, entry.serialize()?));
        self.cursor += 1;
        if self.cursor >= self.max_file_size {
          self.checkpoint_c.send(());
          self.cursor %= self.max_file_size
        }
      }

      self.current.append(record);
    }
    pages.push((self.cursor, self.current.serialize()?));

    return Ok(self.write_c.send_with_done(pages));
  }
}
