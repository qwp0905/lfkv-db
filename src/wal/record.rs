use crate::{
  disk::{Page, PAGE_SIZE},
  Error,
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
    usize, // last log id
  ),
}
impl Operation {
  pub fn type_u8(&self) -> u8 {
    match self {
      Operation::Insert(_, _) => 1,
      Operation::Start => 2,
      Operation::Commit => 3,
      Operation::Abort => 4,
      Operation::Checkpoint(_) => 5,
    }
  }
  fn to_bytes(&self) -> Vec<u8> {
    let mut v = vec![self.type_u8()];
    match self {
      Operation::Insert(index, page) => {
        v.extend_from_slice(&index.to_le_bytes());
        v.extend_from_slice(page.as_ref());
      }
      Operation::Checkpoint(log_id) => {
        v.extend_from_slice(&log_id.to_le_bytes());
      }
      _ => {}
    };
    v
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

  pub fn new_checkpoint(log_id: usize, last_log_id: usize) -> Self {
    LogRecord::new(log_id, 0, Operation::Checkpoint(last_log_id))
  }

  pub fn to_bytes_with_len(&self) -> Vec<u8> {
    let mut vec = vec![0, 0, 0, 0, 0, 0]; // len + checksum

    vec.extend_from_slice(&self.log_id.to_le_bytes());
    vec.extend_from_slice(&self.tx_id.to_le_bytes());
    vec.extend_from_slice(&self.operation.to_bytes());
    let len = ((vec.len() - 2) as u16).to_le_bytes();

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&vec[6..]);

    vec[2..6].copy_from_slice(&hasher.finalize().to_le_bytes());
    vec[..2].copy_from_slice(&len);
    vec
  }

  pub fn to_bytes(&self) -> Vec<u8> {
    let mut vec = vec![0, 0, 0, 0]; // checksum

    vec.extend_from_slice(&self.log_id.to_le_bytes());
    vec.extend_from_slice(&self.tx_id.to_le_bytes());
    vec.extend_from_slice(&self.operation.to_bytes());

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&vec[4..]);
    vec[0..4].copy_from_slice(&hasher.finalize().to_le_bytes());
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
      return Err(Error::InvalidFormat("log record too short."));
    }

    let checksum = u32::from_le_bytes(
      value[0..4]
        .try_into()
        .map_err(|_| Error::InvalidFormat("cannot read checksum from log record"))?,
    );
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&value[4..]);
    if hasher.finalize() != checksum {
      return Err(Error::InvalidFormat("checksum not matched."));
    }

    let log_id = usize::from_le_bytes(
      value[4..12]
        .try_into()
        .map_err(|_| Error::InvalidFormat("invalid log id in log record"))?,
    );
    let tx_id = usize::from_le_bytes(
      value[12..20]
        .try_into()
        .map_err(|_| Error::InvalidFormat("invalid tx id in log record"))?,
    );
    let operation = match value[20] {
      1 => {
        if len != PAGE_SIZE + 29 {
          return Err(Error::InvalidFormat("invalid len for insert log."));
        }
        let index = usize::from_le_bytes(
          value[21..29]
            .try_into()
            .map_err(|_| Error::InvalidFormat("invalid index for insert log."))?,
        );
        let data = value[29..]
          .try_into()
          .map_err(|_| Error::InvalidFormat("invalid data for insert log."))?;
        Operation::Insert(index, data)
      }
      2 => Operation::Start,
      3 => Operation::Commit,
      4 => Operation::Abort,
      5 => {
        if len < 29 {
          return Err(Error::InvalidFormat("checkpoint log too short."));
        }
        let log_id = usize::from_le_bytes(
          value[21..29]
            .try_into()
            .map_err(|_| Error::InvalidFormat("invalid log id for checkpoint log."))?,
        );
        Operation::Checkpoint(log_id)
      }
      _ => return Err(Error::InvalidFormat("invalid type log record.")),
    };
    Ok(LogRecord::new(log_id, tx_id, operation))
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
    let r = LogRecord::new_checkpoint(5, 200);
    let parsed = assert_roundtrip(&r);
    match parsed.operation {
      Operation::Checkpoint(last_log_id) => {
        assert_eq!(last_log_id, 200);
      }
      _ => panic!("expected Checkpoint"),
    }
  }

  #[test]
  fn test_entry_roundtrip() {
    let page = Page::new();
    let mut writer = page.writer();

    let _ = writer.write(&(3 as u16).to_le_bytes());
    let r1 = LogRecord::new_start(1, 1);
    let r2 = LogRecord::new_insert(2, 1, 10, Page::new());
    let r3 = LogRecord::new_commit(3, 1);
    let _ = writer.write(&r1.to_bytes_with_len());
    let _ = writer.write(&r2.to_bytes_with_len());
    let _ = writer.write(&r3.to_bytes_with_len());

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
  fn test_invalid_format() {
    let short: Vec<u8> = vec![0; 10];
    assert!(LogRecord::try_from(short).is_err());

    let mut bad_op = vec![0u8; 17];
    bad_op[16] = 255; // invalid operation type
    assert!(LogRecord::try_from(bad_op).is_err());
  }
}
