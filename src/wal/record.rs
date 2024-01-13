use crate::{
  disk::{Page, Serializable},
  error::Error,
};

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
