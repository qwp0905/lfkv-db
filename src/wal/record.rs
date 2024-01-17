use std::slice::Iter;

use crate::{
  disk::{Page, PageScanner, PageWriter, Serializable},
  error::Error,
  PAGE_SIZE,
};

use super::WAL_PAGE_SIZE;

pub static HEADER_INDEX: usize = 0;

#[derive(Debug)]
pub struct WALFileHeader {
  pub last_index: usize,
  pub applied: usize,
  pub last_transaction: usize,
}

impl WALFileHeader {
  pub fn new(
    last_index: usize,
    applied: usize,
    last_transaction: usize,
  ) -> Self {
    Self {
      last_transaction,
      last_index,
      applied,
    }
  }
}
impl Serializable for WALFileHeader {
  fn serialize(&self) -> Result<Page, Error> {
    let mut page = Page::new();
    let mut wt = page.writer();
    wt.write(&self.last_index.to_be_bytes())?;
    wt.write(&self.applied.to_be_bytes())?;
    wt.write(&self.last_transaction.to_be_bytes())?;
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

#[derive(Debug)]
pub struct RecordHeader {
  log_index: usize,
  transaction_id: usize,
  page_index: usize,
}
impl Serializable for RecordHeader {
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

#[derive(Debug)]
pub struct WALRecord {
  header: RecordHeader,
  data: Page,
}
impl WALRecord {
  pub fn new(
    log_index: usize,
    transaction_id: usize,
    page_index: usize,
    data: Page,
  ) -> Self {
    Self {
      header: RecordHeader {
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

  pub fn get_page_index(&self) -> usize {
    self.header.page_index
  }
}
impl TryFrom<WALRecord> for (Page, Page) {
  type Error = Error;
  fn try_from(value: WALRecord) -> Result<Self, Self::Error> {
    Ok((value.header.serialize()?, value.data))
  }
}
impl TryFrom<(Page, Page)> for WALRecord {
  type Error = Error;
  fn try_from((header, data): (Page, Page)) -> Result<Self, Self::Error> {
    Ok(Self {
      header: header.deserialize()?,
      data,
    })
  }
}
impl From<WALRecord> for Page {
  fn from(value: WALRecord) -> Self {
    value.data
  }
}
impl Clone for WALRecord {
  fn clone(&self) -> Self {
    Self {
      header: RecordHeader {
        log_index: self.header.log_index,
        transaction_id: self.header.transaction_id,
        page_index: self.header.page_index,
      },
      data: self.data.copy(),
    }
  }
}

pub struct InsertRecord {
  index: usize,
  before: Page,
  after: Page,
}
impl InsertRecord {
  pub fn new(index: usize, before: Page, after: Page) -> Self {
    Self {
      index,
      before,
      after,
    }
  }
}

pub enum Op {
  Start,
  Insert(InsertRecord),
  Commit,
  Checkpoint(usize),
}
impl Op {
  fn read_from(sc: &mut PageScanner<WAL_PAGE_SIZE>) -> crate::Result<Self> {
    let i = sc.read()?;
    match i {
      0 => Ok(Self::Start),
      1 => {
        let index = sc.read_usize()?;
        let before = sc.read_n(PAGE_SIZE)?.into();
        let after = sc.read_n(PAGE_SIZE)?.into();
        Ok(Self::Insert(InsertRecord {
          index,
          before,
          after,
        }))
      }
      2 => Ok(Self::Commit),
      3 => {
        let index = sc.read_usize()?;
        Ok(Self::Checkpoint(index))
      }
      _ => Err(Error::Invalid),
    }
  }

  fn write_to(&self, wt: &mut PageWriter<WAL_PAGE_SIZE>) -> crate::Result<()> {
    match self {
      Self::Start => wt.write(&0usize.to_be_bytes())?,
      Self::Insert(record) => {
        wt.write(&1usize.to_be_bytes())?;
        wt.write(&record.index.to_be_bytes())?;
        wt.write(record.before.as_ref())?;
        wt.write(record.after.as_ref())?;
      }
      Self::Commit => wt.write(&2usize.to_be_bytes())?,
      Self::Checkpoint(i) => {
        wt.write(&3usize.to_be_bytes())?;
        wt.write(&i.to_be_bytes())?;
      }
    }
    Ok(())
  }
}

pub struct Record {
  pub transaction_id: usize,
  pub index: usize,
  pub operation: Op,
}
impl Record {
  pub fn new(transaction_id: usize, index: usize, operation: Op) -> Self {
    Self {
      transaction_id,
      index,
      operation,
    }
  }

  fn size(&self) -> usize {
    17 + match self.operation {
      Op::Insert(_) => PAGE_SIZE * 2 + 8,
      Op::Checkpoint(_) => 8,
      _ => 0,
    }
  }

  pub fn is_insert(&self) -> Option<(usize, Page)> {
    if let Op::Insert(ir) = &self.operation {
      return Some((ir.index, ir.after.copy()));
    }
    return None;
  }
}

pub struct RecordEntry {
  records: Vec<Record>,
}
impl RecordEntry {
  pub fn new() -> Self {
    Self { records: vec![] }
  }

  pub fn is_available(&self, record: &Record) -> bool {
    return self.records.iter().fold(0, |a, r| a + r.size()) + record.size()
      > WAL_PAGE_SIZE - 20;
  }

  pub fn append(&mut self, record: Record) {
    self.records.push(record);
  }

  pub fn iter(&self) -> RecordEntryIter<'_> {
    RecordEntryIter {
      inner: self.records.iter(),
    }
  }
}
impl Serializable<Error, WAL_PAGE_SIZE> for RecordEntry {
  fn deserialize(value: &Page<WAL_PAGE_SIZE>) -> Result<Self, Error> {
    let mut sc = value.scanner();
    let mut records = vec![];
    let len = sc.read_usize()?;
    for _ in 0..len {
      let transaction_id = sc.read_usize()?;
      let index = sc.read_usize()?;
      let operation = Op::read_from(&mut sc)?;
      records.push(Record::new(transaction_id, index, operation))
    }
    return Ok(Self { records });
  }
  fn serialize(&self) -> Result<Page<WAL_PAGE_SIZE>, Error> {
    let mut p = Page::new();
    let mut wt = p.writer();
    wt.write(&self.records.len().to_be_bytes())?;
    for record in &self.records {
      wt.write(&record.transaction_id.to_be_bytes())?;
      wt.write(&record.index.to_be_bytes())?;
      record.operation.write_to(&mut wt)?;
    }
    return Ok(p);
  }
}

pub struct RecordEntryIter<'a> {
  inner: Iter<'a, Record>,
}
impl<'a> Iterator for RecordEntryIter<'a> {
  type Item = &'a Record;
  fn next(&mut self) -> Option<Self::Item> {
    self.inner.next()
  }
}

#[cfg(test)]
mod tests {
  use crate::{Page, Serializable};

  use super::WALFileHeader;

  #[test]
  fn _1() {
    let mut b = [0; 4096];
    b[0] = 1;
    b[1..9].copy_from_slice(&(1 as usize).to_be_bytes());
    b[9..17].copy_from_slice(&(0 as usize).to_be_bytes());
    b[17..25].copy_from_slice(&(100 as usize).to_be_bytes());
    let r = WALFileHeader {
      last_index: 1,
      last_transaction: 100,
      applied: 0,
    };
    assert_eq!(r.serialize().ok(), Some(Page::from(b)))
  }
}
