use crate::{disk::Page, error::ErrorKind};

pub static HEADER_INDEX: usize = 0;

pub struct FileHeader {
  pub last_index: usize,
  pub applied: usize,
  pub last_transaction: usize,
}

impl FileHeader {
  pub fn new(
    last_index: usize,
    applied: usize,
    last_transaction: usize,
  ) -> Self {
    Self {
      last_index,
      applied,
      last_transaction,
    }
  }
}
impl TryFrom<Page> for FileHeader {
  type Error = ErrorKind;
  fn try_from(value: Page) -> Result<Self, Self::Error> {
    let mut sc = value.scanner();
    let last_index = sc.read_usize();
    let applied = sc.read_usize();
    let last_transaction = sc.read_usize();

    Ok(Self::new(last_index, applied, last_transaction))
  }
}
impl From<FileHeader> for Page {
  fn from(value: FileHeader) -> Self {
    let mut page = Page::new();
    let mut wt = page.writer();
    wt.write(&value.last_index.to_be_bytes()).unwrap();
    wt.write(&value.applied.to_be_bytes()).unwrap();
    wt.write(&value.last_transaction.to_be_bytes()).unwrap();
    return page;
  }
}

pub enum Operation {
  Insert = 0,
  Start = 1,
  Commit = 2,
  Abort = 3,
  CheckPoint = 4,
}

pub struct LogEntryHeader {
  log_index: usize,
  transaction_id: usize,
  page_index: usize,
  op: Operation,
}
impl From<LogEntryHeader> for Page {
  fn from(value: LogEntryHeader) -> Self {
    let mut p = Page::new();
    let mut wt = p.writer();
    wt.write(&[value.op as u8]).unwrap();
    wt.write(&value.log_index.to_be_bytes()).unwrap();
    wt.write(&value.transaction_id.to_be_bytes()).unwrap();
    wt.write(&value.page_index.to_be_bytes()).unwrap();
    return p;
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
impl From<LogEntry> for (Page, Page) {
  fn from(value: LogEntry) -> Self {
    (value.header.into(), value.data)
  }
}
