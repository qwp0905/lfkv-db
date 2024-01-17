use std::{
  sync::{Arc, Mutex},
  time::Duration,
};

use crate::{
  disk::PageSeeker, ContextReceiver, Page, Result, ShortenedMutex,
  StoppableChannel, ThreadPool,
};

use super::{InsertRecord, Op, Record, RotateWriter, WAL_PAGE_SIZE};

pub struct WAL {
  core: Mutex<Core>,
}

impl WAL {
  pub fn next_transaction(&self) -> Result<usize> {
    self.core.l().next_transaction()
  }

  pub fn append(
    &self,
    tx_id: usize,
    page_index: usize,
    before: Page,
    after: Page,
  ) -> Result<()> {
    self.core.l().append(tx_id, page_index, before, after)
  }
}

struct Core {
  wal_disk: Arc<Mutex<RotateWriter>>,
  last_index: usize,
  last_transaction: usize,
  max_buffer_size: usize,
  checkpoint_c: StoppableChannel<()>,
  background: ThreadPool<Result<()>>,
}
impl Core {
  fn new(
    wal_disk: Arc<Mutex<RotateWriter>>,
    last_index: usize,
    last_transaction: usize,
    max_buffer_size: usize,
    checkpoint_c: StoppableChannel<()>,
  ) -> Self {
    Self {
      wal_disk,
      last_index,
      last_transaction,
      max_buffer_size: max_buffer_size / WAL_PAGE_SIZE,
      checkpoint_c,
      background: ThreadPool::new(2, max_buffer_size * 6 / 5, "wal", None),
    }
  }

  fn start_background(
    &self,
    disk: Arc<PageSeeker>,
    rx: ContextReceiver<()>,
    timeout: Duration,
  ) {
    let wal_disk = self.wal_disk.clone();
    self.background.schedule(move || {
      while let Ok(_) = rx.recv_new_or_timeout(timeout) {
        let flushed = { wal_disk.l().drain_buffer() };
        if flushed.len() == 0 {
          continue;
        }

        let mut applied = 0;
        for entry in flushed {
          for record in entry.iter() {
            if let Some((index, page)) = record.is_insert() {
              disk.write(index, page)?;
            }
            applied = record.index;
          }
        }

        wal_disk
          .l()
          .append(Record::new(0, 0, Op::Checkpoint(applied)))?;
      }
      Ok(())
    });
  }

  fn next_transaction(&mut self) -> Result<usize> {
    let index = self.last_index + 1;
    let tx_id = self.last_transaction + 1;
    let record = Record::new(tx_id, index, Op::Start);
    self.wal_disk.l().append(record)?;
    self.last_index = index;
    self.last_transaction = tx_id;
    return Ok(tx_id);
  }

  fn append(
    &mut self,
    tx_id: usize,
    page_index: usize,
    before: Page,
    after: Page,
  ) -> Result<()> {
    let index = self.last_index + 1;
    let record = Record::new(
      tx_id,
      index,
      Op::Insert(InsertRecord::new(page_index, before, after)),
    );
    self.wal_disk.l().append(record)?;
    self.last_index = index;
    return Ok(());
  }
}
