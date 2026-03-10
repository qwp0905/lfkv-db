use std::{ptr::copy_nonoverlapping, slice::from_raw_parts};

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
    usize, // min active version
  ),
}
impl Operation {
  fn type_byte(&self) -> u8 {
    match self {
      Operation::Insert(_, _) => 1,
      Operation::Start => 2,
      Operation::Commit => 3,
      Operation::Abort => 4,
      Operation::Checkpoint(_, _) => 5,
    }
  }

  const INSERT_LEN: u16 = 1 + 8 + (PAGE_SIZE as u16);
  const CHECKPOINT_LEN: u16 = 1 + 8 + 8;
  const OTHER_LEN: u16 = 1;
  fn byte_len(&self) -> u16 {
    match self {
      Operation::Insert(_, _) => Self::INSERT_LEN,
      Operation::Checkpoint(_, _) => Self::CHECKPOINT_LEN,
      _ => Self::OTHER_LEN,
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

  pub fn new_checkpoint(log_id: usize, last_log_id: usize, min_active: usize) -> Self {
    LogRecord::new(log_id, 0, Operation::Checkpoint(last_log_id, min_active))
  }

  unsafe fn write_at(&self, ptr: *mut u8) {
    let mut offset = 4;
    copy_nonoverlapping(self.log_id.to_le_bytes().as_ptr(), ptr.add(offset), 8);
    offset += 8;

    copy_nonoverlapping(self.tx_id.to_le_bytes().as_ptr(), ptr.add(offset), 8);
    offset += 8;

    *ptr.add(offset) = self.operation.type_byte();
    offset += 1;
    match &self.operation {
      Operation::Insert(index, page) => {
        copy_nonoverlapping(index.to_le_bytes().as_ptr(), ptr.add(offset), 8);
        offset += 8;
        copy_nonoverlapping(page.as_ptr(), ptr.add(offset), PAGE_SIZE);
        offset += PAGE_SIZE;
      }
      Operation::Checkpoint(log_id, min_active) => {
        copy_nonoverlapping(log_id.to_le_bytes().as_ptr(), ptr.add(offset), 8);
        offset += 8;
        copy_nonoverlapping(min_active.to_le_bytes().as_ptr(), ptr.add(offset), 8);
        offset += 8;
      }
      Operation::Start => {}
      Operation::Commit => {}
      Operation::Abort => {}
    };

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(unsafe { from_raw_parts(ptr.add(4), offset - 4) });
    let checksum = hasher.finalize().to_le_bytes();
    unsafe { copy_nonoverlapping(checksum.as_ptr(), ptr, 4) };
  }

  fn byte_len(&self) -> u16 {
    self.operation.byte_len() + 4 + 8 + 8
  }
  pub fn to_bytes_with_len(&self) -> Vec<u8> {
    let len = self.byte_len();
    let mut vec = vec![0; (len + 2) as usize];
    let ptr = vec.as_mut_ptr();
    unsafe { copy_nonoverlapping((len as u16).to_le_bytes().as_ptr(), ptr, 2) };
    unsafe { self.write_at(ptr.add(2)) };
    vec
  }

  pub fn to_bytes(&self) -> Vec<u8> {
    let len = self.byte_len();
    let mut vec = vec![0; len as usize];
    unsafe { self.write_at(vec.as_mut_ptr()) };
    vec
  }
}

impl TryFrom<&[u8]> for LogRecord {
  type Error = Error;

  fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
    let len = value.len();
    if len < 21 {
      return Err(Error::InvalidFormat("log record too short."));
    }
    let ptr = value.as_ptr();

    let checksum = u32::from_le_bytes(unsafe { (ptr as *const [u8; 4]).read() });
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(unsafe { from_raw_parts(ptr.add(4), len - 4) });
    if hasher.finalize() != checksum {
      return Err(Error::InvalidFormat("checksum not matched."));
    }

    let log_id = usize::from_le_bytes(unsafe { (ptr.add(4) as *const [u8; 8]).read() });
    let tx_id = usize::from_le_bytes(unsafe { (ptr.add(12) as *const [u8; 8]).read() });
    let operation = match unsafe { *ptr.add(20) } {
      1 => {
        if len != PAGE_SIZE + 29 {
          return Err(Error::InvalidFormat("invalid len for insert log."));
        }
        let index =
          usize::from_le_bytes(unsafe { (ptr.add(21) as *const [u8; 8]).read() });

        let data = [0; PAGE_SIZE];
        unsafe { copy_nonoverlapping(ptr.add(29), data.as_ptr() as *mut u8, PAGE_SIZE) };
        Operation::Insert(index, Page::from(data))
      }
      2 => {
        if len != 21 {
          return Err(Error::InvalidFormat("invalid len for start log."));
        }
        Operation::Start
      }
      3 => {
        if len != 21 {
          return Err(Error::InvalidFormat("invalid len for commit log."));
        }
        Operation::Commit
      }
      4 => {
        if len != 21 {
          return Err(Error::InvalidFormat("invalid len for abort log."));
        };
        Operation::Abort
      }
      5 => {
        if len != 37 {
          return Err(Error::InvalidFormat("invalid len for checkpoint log."));
        }
        let log_id =
          usize::from_le_bytes(unsafe { (ptr.add(21) as *const [u8; 8]).read() });
        let min_active =
          usize::from_le_bytes(unsafe { (ptr.add(29) as *const [u8; 8]).read() });
        Operation::Checkpoint(log_id, min_active)
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
      match scanner.read_n(size as usize).and_then(|p| p.try_into()) {
        Ok(record) => data.push(record),
        Err(_) => return (data, true), // ignore error cause of partial write
      }
    }
    (data, false)
  }
}

#[cfg(test)]
#[path = "tests/record.rs"]
mod tests;
