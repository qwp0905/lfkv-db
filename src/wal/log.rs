use crate::{
  disk::{Page, Serializable},
  error::Error,
};

pub static HEADER_INDEX: usize = 0;

pub struct FileHeader {
  pub next_index: usize,
  pub applied: usize,
  pub next_transaction: usize,
}

impl FileHeader {
  pub fn new(
    next_index: usize,
    applied: usize,
    next_transaction: usize,
  ) -> Self {
    Self {
      next_transaction,
      next_index,
      applied,
    }
  }
}
impl Serializable for FileHeader {
  fn serialize(&self) -> Result<Page, Error> {
    let mut page = Page::new();
    let mut wt = page.writer();
    wt.write(&self.next_index.to_be_bytes())?;
    wt.write(&self.applied.to_be_bytes())?;
    return Ok(page);
  }

  fn deserialize(value: &Page) -> Result<Self, Error> {
    let mut sc = value.scanner();
    let last_index = sc.read_usize()?;
    let applied = sc.read_usize()?;
    let last_transaction = sc.read_usize()?;

    Ok(Self::new(last_index, applied, last_transaction))
  }
}

pub struct LogEntryHeader {
  log_index: usize,
  transaction_id: usize,
  page_index: usize,
}
impl Serializable for LogEntryHeader {
  fn deserialize(value: &Page) -> Result<Self, Error> {
    let mut sc = value.scanner();
    let log_index = sc.read_usize()?;
    let transaction_id = sc.read_usize()?;
    let page_index = sc.read_usize()?;
    return Ok(Self {
      log_index,
      transaction_id,
      page_index,
    });
  }
  fn serialize(&self) -> Result<Page, Error> {
    let mut p = Page::new();
    let mut wt = p.writer();
    wt.write(&self.log_index.to_be_bytes())?;
    wt.write(&self.transaction_id.to_be_bytes())?;
    wt.write(&self.page_index.to_be_bytes())?;
    return Ok(p);
  }
}

pub struct LogEntry {
  header: LogEntryHeader,
  data: Page,
}
impl LogEntry {
  pub fn new(
    log_index: usize,
    transaction_id: usize,
    page_index: usize,
    data: Page,
  ) -> Self {
    Self {
      header: LogEntryHeader {
        log_index,
        transaction_id,
        page_index,
      },
      data,
    }
  }

  pub fn get_index(&self) -> usize {
    self.header.log_index
  }

  pub fn take_data(self) -> Page {
    self.data
  }
}
impl TryFrom<LogEntry> for (Page, Page) {
  type Error = Error;
  fn try_from(value: LogEntry) -> Result<Self, Self::Error> {
    Ok((value.header.serialize()?, value.data))
  }
}
impl From<LogEntry> for Page {
  fn from(value: LogEntry) -> Self {
    value.data
  }
}
impl Clone for LogEntry {
  fn clone(&self) -> Self {
    Self {
      header: LogEntryHeader {
        log_index: self.header.log_index,
        transaction_id: self.header.transaction_id,
        page_index: self.header.page_index,
      },
      data: self.data.copy(),
    }
  }
}
