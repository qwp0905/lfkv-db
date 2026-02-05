use std::ops::Add;

use crate::{Error, Page, Result, PAGE_SIZE};

pub const WAL_BLOCK_SIZE: usize = 16 << 10; // 16kb

pub enum Operation {
  Insert(
    usize, // index of the page
    Page,  // data
  ),
  Start,
  Commit,
  Abort,
  Checkpoint,
}

pub struct LogRecord {
  pub log_id: usize,
  pub tx_id: usize,
  pub operation: Operation,
}
impl LogRecord {
  #[inline]
  fn new(log_id: usize, tx_id: usize, operation: Operation) -> Self {
    LogRecord {
      tx_id,
      operation,
      log_id,
    }
  }
  pub fn new_insert(log_id: usize, tx_id: usize, page_index: usize, data: Page) -> Self {
    LogRecord::new(log_id, tx_id, Operation::Insert(page_index, data))
  }

  pub fn new_start(log_id: usize, tx_id: usize) -> Self {
    LogRecord::new(log_id, tx_id, Operation::Start)
  }

  pub fn new_commit(log_id: usize, tx_id: usize) -> Self {
    LogRecord::new(log_id, tx_id, Operation::Commit)
  }

  pub fn new_abort(log_id: usize, tx_id: usize) -> Self {
    LogRecord::new(log_id, tx_id, Operation::Abort)
  }

  pub fn new_checkpoint(log_id: usize, tx_id: usize) -> Self {
    LogRecord::new(log_id, tx_id, Operation::Checkpoint)
  }

  pub fn to_bytes(&self) -> Vec<u8> {
    let mut vec = Vec::new();

    vec.extend_from_slice(&self.log_id.to_le_bytes());
    vec.extend_from_slice(&self.tx_id.to_le_bytes());

    match &self.operation {
      Operation::Insert(index, page) => {
        vec.push(0);
        vec.extend_from_slice(&index.to_le_bytes());
        vec.extend_from_slice(page.as_ref());
      }
      Operation::Start => vec.push(1),
      Operation::Commit => vec.push(2),
      Operation::Abort => vec.push(3),
      Operation::Checkpoint => vec.push(4),
    }
    vec
  }
}
impl From<LogRecord> for Vec<u8> {
  fn from(value: LogRecord) -> Self {
    value.to_bytes()
  }
}
impl TryFrom<Vec<u8>> for LogRecord {
  type Error = Error;

  fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
    let len = value.len();
    if len.lt(&17) {
      return Err(Error::Invalid);
    }
    let log_id =
      usize::from_le_bytes(value[0..8].try_into().map_err(|_| Error::Invalid)?);
    let tx_id =
      usize::from_le_bytes(value[8..16].try_into().map_err(|_| Error::Invalid)?);
    let operation = match value[16] {
      0 => {
        if len.ne(&PAGE_SIZE.add(17).add(8)) {
          return Err(Error::Invalid);
        }
        let index =
          usize::from_le_bytes(value[17..25].try_into().map_err(|_| Error::Invalid)?);
        let data = value[25..].try_into().map_err(|_| Error::Invalid)?;
        Operation::Insert(index, data)
      }
      1 => Operation::Start,
      2 => Operation::Commit,
      3 => Operation::Abort,
      4 => Operation::Checkpoint,
      _ => return Err(Error::Invalid),
    };
    Ok(LogRecord::new(log_id, tx_id, operation))
  }
}

pub struct LogEntry {
  data: Vec<u8>,
  index: usize,
}
impl LogEntry {
  pub fn new(index: usize) -> Self {
    Self {
      data: vec![0; 2],
      index,
    }
  }

  pub fn get_index(&self) -> usize {
    self.index
  }

  pub fn append(&mut self, record: &LogRecord) -> Result<()> {
    let buf = record.to_bytes();
    if self.data.len().add(buf.len().add(2)).gt(&WAL_BLOCK_SIZE) {
      return Err(Error::EOF);
    }
    let len = u16::from_le_bytes(self.data[0..2].try_into().unwrap());
    self
      .data
      .extend_from_slice(&(buf.len() as u16).to_le_bytes());
    self.data.extend_from_slice(&buf);
    self.data[0..2].copy_from_slice(&len.add(1).to_le_bytes());
    Ok(())
  }
}
impl From<LogEntry> for Page<WAL_BLOCK_SIZE> {
  fn from(value: LogEntry) -> Self {
    value.data.into()
  }
}
impl TryFrom<&Page<WAL_BLOCK_SIZE>> for Vec<LogRecord> {
  type Error = Error;

  fn try_from(value: &Page<WAL_BLOCK_SIZE>) -> std::result::Result<Self, Self::Error> {
    let mut scanner = value.scanner();
    let len = scanner.read_u16()?;
    let mut data = vec![];
    for _ in 0..len {
      let size = scanner.read_u16()?;
      let record = scanner.read_n(size as usize)?.to_vec().try_into()?;
      data.push(record);
    }
    Ok(data)
  }
}
impl AsRef<[u8]> for LogEntry {
  fn as_ref(&self) -> &[u8] {
    &self.data
  }
}
