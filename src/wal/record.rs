use crate::{
  disk::{Page, PageWriter},
  size, Error, Serializable, PAGE_SIZE,
};

pub const WAL_PAGE_SIZE: usize = size::kb(16);

#[derive(Debug)]
pub struct InsertLog {
  pub page_index: usize,
  pub data: Page,
}
impl InsertLog {
  fn new(page_index: usize, data: Page) -> Self {
    Self { page_index, data }
  }
}
impl Clone for InsertLog {
  fn clone(&self) -> Self {
    Self {
      page_index: self.page_index,
      data: self.data.copy(),
    }
  }
}

#[derive(Debug, Clone)]
pub enum Operation {
  Start,
  Commit,
  Abort,
  Checkpoint(usize),
  Insert(InsertLog),
}
impl Operation {
  fn size(&self) -> usize {
    match self {
      Operation::Start => 1,
      Operation::Commit => 1,
      Operation::Abort => 1,
      Operation::Checkpoint(_) => 9,
      Operation::Insert(_) => 8 + PAGE_SIZE,
    }
  }
}

pub struct LogRecord {
  pub index: usize,
  pub transaction_id: usize,
  pub operation: Operation,
}
impl LogRecord {
  pub fn new_start(index: usize, transaction_id: usize) -> Self {
    Self::new(index, transaction_id, Operation::Start)
  }

  pub fn new_commit(index: usize, transaction_id: usize) -> Self {
    Self::new(index, transaction_id, Operation::Commit)
  }

  pub fn new_abort(index: usize, transaction_id: usize) -> Self {
    Self::new(index, transaction_id, Operation::Commit)
  }

  pub fn new_insert(
    index: usize,
    transaction_id: usize,
    page_index: usize,
    data: Page,
  ) -> Self {
    Self::new(
      index,
      transaction_id,
      Operation::Insert(InsertLog::new(page_index, data)),
    )
  }

  pub fn new_checkpoint(index: usize, applied: usize) -> Self {
    Self::new(index, 0, Operation::Checkpoint(applied))
  }

  fn new(index: usize, transaction_id: usize, operation: Operation) -> Self {
    Self {
      index,
      transaction_id,
      operation,
    }
  }

  fn size(&self) -> usize {
    self.operation.size() + 16
  }

  fn write_to(&self, wt: &mut PageWriter<WAL_PAGE_SIZE>) -> crate::Result<()> {
    wt.write(&self.index.to_be_bytes())?;
    wt.write(&self.transaction_id.to_be_bytes())?;
    match &self.operation {
      Operation::Start => {
        wt.write(&[0])?;
      }
      Operation::Commit => {
        wt.write(&[1])?;
      }
      Operation::Abort => {
        wt.write(&[2])?;
      }
      Operation::Checkpoint(i) => {
        wt.write(&[3])?;
        wt.write(&i.to_be_bytes())?;
      }
      Operation::Insert(log) => {
        wt.write(&[4])?;
        wt.write(&log.page_index.to_be_bytes())?;
        wt.write(log.data.as_ref())?;
      }
    }
    Ok(())
  }
}

pub struct LogEntry {
  pub records: Vec<LogRecord>,
}
impl LogEntry {
  pub fn new(records: Vec<LogRecord>) -> Self {
    Self { records }
  }

  pub fn is_available(&self, record: &LogRecord) -> bool {
    self.records.iter().fold(0, |a, r| a + r.size()) + record.size()
      <= WAL_PAGE_SIZE - 30
  }
}
impl Default for LogEntry {
  fn default() -> Self {
    Self {
      records: Default::default(),
    }
  }
}

impl Serializable<Error, WAL_PAGE_SIZE> for LogEntry {
  fn serialize(&self) -> Result<Page<WAL_PAGE_SIZE>, Error> {
    let mut page = Page::new();
    let mut wt = page.writer();
    wt.write(&self.records.len().to_be_bytes())?;
    for record in &self.records {
      record.write_to(&mut wt)?;
    }
    Ok(page)
  }
  fn deserialize(value: &Page<WAL_PAGE_SIZE>) -> Result<Self, Error> {
    let records = vec![];
    let mut sc = value.scanner();
    let l = sc.read_usize()?;
    for _ in 0..l {
      todo!();
    }

    Ok(Self { records })
  }
}
