use std::ops::Add;

use crate::{
  disk::{Page, PAGE_SIZE},
  Error, Result,
};

pub const WAL_BLOCK_SIZE: usize = 16 << 10; // 16kb

pub enum Operation {
  Insert(
    usize, // index of the page
    Page,  // data
  ),
  Start,
  Commit,
  Abort,
  Checkpoint(usize, usize),
  Free(usize),
}
impl Operation {
  fn to_bytes(&self) -> Vec<u8> {
    match self {
      Operation::Insert(index, page) => {
        let mut v = vec![0];
        v.extend_from_slice(&index.to_le_bytes());
        v.extend_from_slice(page.as_ref());
        v
      }
      Operation::Start => vec![1],
      Operation::Commit => vec![2],
      Operation::Abort => vec![3],
      Operation::Checkpoint(index, log_id) => {
        let mut v = vec![4];
        v.extend_from_slice(&log_id.to_le_bytes());
        v.extend_from_slice(&index.to_le_bytes());
        v
      }
      Operation::Free(index) => {
        let mut v = vec![5];
        v.extend_from_slice(&index.to_le_bytes());
        v
      }
    }
  }
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

  pub fn new_checkpoint(log_id: usize, last_free: usize, last_log_id: usize) -> Self {
    LogRecord::new(log_id, 0, Operation::Checkpoint(last_free, last_log_id))
  }
  pub fn new_free(log_id: usize, page_index: usize) -> Self {
    LogRecord::new(log_id, 0, Operation::Free(page_index))
  }

  pub fn to_bytes(&self) -> Vec<u8> {
    let mut vec = Vec::new();

    vec.extend_from_slice(&self.log_id.to_le_bytes());
    vec.extend_from_slice(&self.tx_id.to_le_bytes());
    vec.extend_from_slice(&self.operation.to_bytes());
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
      return Err(Error::InvalidFormat);
    }
    let log_id =
      usize::from_le_bytes(value[0..8].try_into().map_err(|_| Error::InvalidFormat)?);
    let tx_id =
      usize::from_le_bytes(value[8..16].try_into().map_err(|_| Error::InvalidFormat)?);
    let operation = match value[16] {
      0 => {
        if len.ne(&PAGE_SIZE.add(17).add(8)) {
          return Err(Error::InvalidFormat);
        }
        let index = usize::from_le_bytes(
          value[17..25].try_into().map_err(|_| Error::InvalidFormat)?,
        );
        let data = value[25..].try_into().map_err(|_| Error::InvalidFormat)?;
        Operation::Insert(index, data)
      }
      1 => Operation::Start,
      2 => Operation::Commit,
      3 => Operation::Abort,
      4 => {
        if len.lt(&33) {
          return Err(Error::InvalidFormat);
        }
        let log_id = usize::from_le_bytes(
          value[17..25].try_into().map_err(|_| Error::InvalidFormat)?,
        );
        let index = usize::from_le_bytes(
          value[25..33].try_into().map_err(|_| Error::InvalidFormat)?,
        );
        Operation::Checkpoint(index, log_id)
      }
      5 => {
        if len.lt(&25) {
          return Err(Error::InvalidFormat);
        }
        let index = usize::from_le_bytes(
          value[17..25].try_into().map_err(|_| Error::InvalidFormat)?,
        );
        Operation::Free(index)
      }
      _ => return Err(Error::InvalidFormat),
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
