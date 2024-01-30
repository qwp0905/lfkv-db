use crate::{disk::PageSeeker, Error, Page, Result, Serializable, PAGE_SIZE};

const UNDO_PAGE_SIZE: usize = PAGE_SIZE + 24;

pub struct UndoLog {
  log_index: usize,
  tx_id: usize,
  data: Page,
  undo_index: usize,
}

impl UndoLog {
  pub fn new(log_index: usize, tx_id: usize, data: Page, undo_index: usize) -> Self {
    Self {
      log_index,
      tx_id,
      data,
      undo_index,
    }
  }
}
impl Serializable<Error, UNDO_PAGE_SIZE> for UndoLog {
  fn serialize(&self) -> core::result::Result<Page<UNDO_PAGE_SIZE>, Error> {
    let mut page = Page::new();
    let mut wt = page.writer();
    wt.write(&self.log_index.to_be_bytes())?;
    wt.write(&self.tx_id.to_be_bytes())?;
    wt.write(&self.undo_index.to_be_bytes())?;
    wt.write(self.data.as_ref())?;

    Ok(page)
  }
  fn deserialize(value: &Page<UNDO_PAGE_SIZE>) -> core::result::Result<Self, Error> {
    let mut sc = value.scanner();
    let log_index = sc.read_usize()?;
    let tx_id = sc.read_usize()?;
    let undo_index = sc.read_usize()?;
    let data = sc.read_n(PAGE_SIZE)?.into();

    Ok(UndoLog::new(log_index, tx_id, data, undo_index))
  }
}

pub struct Rollback {
  disk: PageSeeker<UNDO_PAGE_SIZE>,
  cursor: usize,
}
impl Rollback {
  pub fn get(&self, log_index: usize) -> Result<Page> {
    let undo: UndoLog = self.disk.read(log_index * UNDO_PAGE_SIZE)?.deserialize()?;
    Ok(undo.data)
  }

  pub fn append(&mut self, log: UndoLog) -> Result<usize> {
    let cursor = self.cursor;
    self.cursor += 1;
    self.cursor %= UNDO_PAGE_SIZE;
    self.disk.write(cursor * UNDO_PAGE_SIZE, log.serialize()?)?;
    self.disk.fsync()?;
    Ok(cursor)
  }
}
