use std::ops::Add;

use crate::{
  disk::{Page, PAGE_SIZE},
  Error, Result,
};

pub const WAL_BLOCK_SIZE: usize = 16 << 10; // 16kb

#[derive(Debug)]
pub enum Operation {
  Insert(
    usize, // index of the page
    Page,  // data
  ),
  Start,
  Commit,
  Abort,
  Checkpoint(
    usize, // last free page index
    usize, // last log id
  ),
  Free(usize),
}
impl Operation {
  fn type_u8(&self) -> u8 {
    match self {
      Operation::Insert(_, _) => 1,
      Operation::Start => 2,
      Operation::Commit => 3,
      Operation::Abort => 4,
      Operation::Checkpoint(_, _) => 5,
      Operation::Free(_) => 6,
    }
  }
  fn to_bytes(&self) -> Vec<u8> {
    let mut v = vec![self.type_u8()];
    match self {
      Operation::Insert(index, page) => {
        v.extend_from_slice(&index.to_be_bytes());
        v.extend_from_slice(page.as_ref());
        v
      }
      Operation::Checkpoint(index, log_id) => {
        v.extend_from_slice(&log_id.to_be_bytes());
        v.extend_from_slice(&index.to_be_bytes());
        v
      }
      Operation::Free(index) => {
        v.extend_from_slice(&index.to_be_bytes());
        v
      }
      _ => v,
    }
  }
}

#[derive(Debug)]
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
    let mut vec = vec![0, 0, 0, 0]; // checksum

    vec.extend_from_slice(&self.log_id.to_be_bytes());
    vec.extend_from_slice(&self.tx_id.to_be_bytes());
    vec.extend_from_slice(&self.operation.to_bytes());
    let mut sum = 0u32;
    for i in 4..vec.len() {
      sum += vec[i] as u32;
    }
    vec[0..4].copy_from_slice(&sum.to_be_bytes());
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
    if len < 21 {
      return Err(Error::InvalidFormat);
    }

    let checksum =
      u32::from_be_bytes(value[0..4].try_into().map_err(|_| Error::InvalidFormat)?);
    let mut sum = 0u32;
    for i in 4..len {
      sum += value[i] as u32;
    }
    if sum != checksum {
      return Err(Error::InvalidFormat);
    }

    let log_id =
      usize::from_be_bytes(value[4..12].try_into().map_err(|_| Error::InvalidFormat)?);
    let tx_id =
      usize::from_be_bytes(value[12..20].try_into().map_err(|_| Error::InvalidFormat)?);
    let operation = match value[20] {
      1 => {
        if len != PAGE_SIZE + 29 {
          return Err(Error::InvalidFormat);
        }
        let index = usize::from_be_bytes(
          value[21..29].try_into().map_err(|_| Error::InvalidFormat)?,
        );
        let data = value[29..].try_into().map_err(|_| Error::InvalidFormat)?;
        Operation::Insert(index, data)
      }
      2 => Operation::Start,
      3 => Operation::Commit,
      4 => Operation::Abort,
      5 => {
        if len < 37 {
          return Err(Error::InvalidFormat);
        }
        let log_id = usize::from_be_bytes(
          value[21..29].try_into().map_err(|_| Error::InvalidFormat)?,
        );
        let index = usize::from_be_bytes(
          value[29..37].try_into().map_err(|_| Error::InvalidFormat)?,
        );
        Operation::Checkpoint(index, log_id)
      }
      6 => {
        if len < 29 {
          return Err(Error::InvalidFormat);
        }
        let index = usize::from_be_bytes(
          value[21..29].try_into().map_err(|_| Error::InvalidFormat)?,
        );
        Operation::Free(index)
      }
      _ => return Err(Error::InvalidFormat),
    };
    Ok(LogRecord::new(log_id, tx_id, operation))
  }
}

#[derive(Debug)]
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
    let len = u16::from_be_bytes(self.data[0..2].try_into().unwrap());
    self
      .data
      .extend_from_slice(&(buf.len() as u16).to_be_bytes());
    self.data.extend_from_slice(&buf);
    self.data[0..2].copy_from_slice(&len.add(1).to_be_bytes());
    Ok(())
  }
}
impl From<LogEntry> for Page<WAL_BLOCK_SIZE> {
  fn from(value: LogEntry) -> Self {
    value.data.into()
  }
}
impl From<&Page<WAL_BLOCK_SIZE>> for (Vec<LogRecord>, bool) {
  fn from(value: &Page<WAL_BLOCK_SIZE>) -> Self {
    let mut data = vec![];
    let mut scanner = value.scanner();
    let len = match scanner.read_u16() {
      Ok(l) => l,
      Err(_) => return (data, true), // ignore error cause of partial write
    };
    for _ in 0..len {
      let size = match scanner.read_u16() {
        Ok(s) => s,
        Err(_) => return (data, true), // ignore error cause of partial write
      };
      match scanner
        .read_n(size as usize)
        .and_then(|p| p.to_vec().try_into())
      {
        Ok(record) => data.push(record),
        Err(_) => return (data, true), // ignore error cause of partial write
      }
    }
    (data, false)
  }
}

impl AsRef<[u8]> for LogEntry {
  fn as_ref(&self) -> &[u8] {
    &self.data
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn assert_roundtrip(record: &LogRecord) -> LogRecord {
    let bytes = record.to_bytes();
    let parsed: LogRecord = bytes.try_into().expect("deserialize failed");
    assert_eq!(parsed.log_id, record.log_id);
    assert_eq!(parsed.tx_id, record.tx_id);
    parsed
  }

  #[test]
  fn test_start_roundtrip() {
    let r = LogRecord::new_start(1, 42);
    let parsed = assert_roundtrip(&r);
    assert!(matches!(parsed.operation, Operation::Start));
  }

  #[test]
  fn test_commit_roundtrip() {
    let r = LogRecord::new_commit(2, 42);
    let parsed = assert_roundtrip(&r);
    assert!(matches!(parsed.operation, Operation::Commit));
  }

  #[test]
  fn test_abort_roundtrip() {
    let r = LogRecord::new_abort(3, 42);
    let parsed = assert_roundtrip(&r);
    assert!(matches!(parsed.operation, Operation::Abort));
  }

  #[test]
  fn test_insert_roundtrip() {
    let mut page = Page::new();
    page.as_mut()[0] = 0xAB;
    page.as_mut()[PAGE_SIZE - 1] = 0xCD;

    let r = LogRecord::new_insert(4, 42, 99, page);
    let parsed = assert_roundtrip(&r);
    match parsed.operation {
      Operation::Insert(index, data) => {
        assert_eq!(index, 99);
        assert_eq!(data.as_ref()[0], 0xAB);
        assert_eq!(data.as_ref()[PAGE_SIZE - 1], 0xCD);
      }
      _ => panic!("expected Insert"),
    }
  }

  #[test]
  fn test_checkpoint_roundtrip() {
    let r = LogRecord::new_checkpoint(5, 100, 200);
    let parsed = assert_roundtrip(&r);
    match parsed.operation {
      Operation::Checkpoint(last_free, last_log_id) => {
        assert_eq!(last_free, 100);
        assert_eq!(last_log_id, 200);
      }
      _ => panic!("expected Checkpoint"),
    }
  }

  #[test]
  fn test_free_roundtrip() {
    let r = LogRecord::new_free(6, 55);
    let parsed = assert_roundtrip(&r);
    match parsed.operation {
      Operation::Free(index) => assert_eq!(index, 55),
      _ => panic!("expected Free"),
    }
  }

  #[test]
  fn test_entry_roundtrip() {
    let mut entry = LogEntry::new(1);

    let r1 = LogRecord::new_start(1, 1);
    let r2 = LogRecord::new_insert(2, 1, 10, Page::new());
    let r3 = LogRecord::new_commit(3, 1);
    entry.append(&r1).expect("append r1");
    entry.append(&r2).expect("append r2");
    entry.append(&r3).expect("append r3");

    let page: Page<WAL_BLOCK_SIZE> = entry.into();
    let (d, complete) = (&page).into();
    assert_eq!(complete, false);

    assert_eq!(d.len(), 3);
    assert_eq!(d[0].log_id, 1);
    assert_eq!(d[0].tx_id, 1);
    assert!(matches!(d[0].operation, Operation::Start));
    assert_eq!(d[1].log_id, 2);
    assert_eq!(d[1].tx_id, 1);
    assert!(matches!(d[1].operation, Operation::Insert(10, _)));
    assert_eq!(d[2].log_id, 3);
    assert_eq!(d[2].tx_id, 1);
    assert!(matches!(d[2].operation, Operation::Commit));
  }

  #[test]
  fn test_entry_overflow() {
    let mut entry = LogEntry::new(0);
    let mut count = 0;
    loop {
      let r = LogRecord::new_start(0, 0);
      if entry.append(&r).is_err() {
        break;
      }
      count += 1;
    }
    assert!(count > 0);

    // insert records are larger, so fewer fit before exceeding WAL_BLOCK_SIZE
    let mut entry2 = LogEntry::new(0);
    let r = LogRecord::new_insert(0, 0, 0, Page::new());
    let mut insert_count = 0;
    loop {
      if entry2.append(&r).is_err() {
        break;
      }
      insert_count += 1;
    }
    assert!(insert_count > 0);
    assert!(insert_count < count);
  }

  #[test]
  fn test_invalid_format() {
    let short: Vec<u8> = vec![0; 10];
    assert!(LogRecord::try_from(short).is_err());

    let mut bad_op = vec![0u8; 17];
    bad_op[16] = 255; // invalid operation type
    assert!(LogRecord::try_from(bad_op).is_err());
  }
}
