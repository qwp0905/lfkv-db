use crate::{disk::Page, error::ErrorKind};

pub static HEADER_INDEX: usize = 0;

pub struct FileHeader {
  pub last_index: usize,
  pub applied: usize,
}

impl FileHeader {
  pub fn new(last_index: usize, applied: usize) -> Self {
    Self {
      last_index,
      applied,
    }
  }
}
impl TryFrom<Page> for FileHeader {
  type Error = ErrorKind;
  fn try_from(value: Page) -> Result<Self, Self::Error> {
    let mut sc = value.scanner();
    let last_index = sc.read_usize()?;
    let applied = sc.read_usize()?;

    Ok(Self::new(last_index, applied))
  }
}
impl TryFrom<FileHeader> for Page {
  type Error = ErrorKind;
  fn try_from(value: FileHeader) -> Result<Self, Self::Error> {
    let mut page = Page::new();
    let mut wt = page.writer();
    wt.write(&value.last_index.to_be_bytes())?;
    wt.write(&value.applied.to_be_bytes())?;
    return Ok(page);
  }
}

pub enum Operation {
  Insert = 0,
  Remove = 1,
}

pub struct LogEntryHeader {
  log_index: usize,
  transaction_id: usize,
  page_index: usize,
  op: Operation,
}
impl TryFrom<LogEntryHeader> for Page {
  type Error = ErrorKind;
  fn try_from(value: LogEntryHeader) -> Result<Self, Self::Error> {
    let mut p = Page::new();
    let mut wt = p.writer();
    wt.write(&[value.op as u8])?;
    wt.write(&value.log_index.to_be_bytes())?;
    wt.write(&value.transaction_id.to_be_bytes())?;
    wt.write(&value.page_index.to_be_bytes())?;
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
    op: Operation,
    data: Page,
  ) -> Self {
    Self {
      header: LogEntryHeader {
        log_index,
        transaction_id,
        page_index,
        op,
      },
      data,
    }
  }
}
impl TryFrom<LogEntry> for (Page, Page) {
  type Error = ErrorKind;
  fn try_from(value: LogEntry) -> Result<Self, Self::Error> {
    Ok((value.header.try_into()?, value.data))
  }
}
