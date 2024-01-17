use std::{
  collections::BTreeMap,
  path::Path,
  sync::{Arc, Mutex},
  time::Duration,
};

use crate::{
  disk::PageSeeker, logger, ContextReceiver, Page, Result, ShortenedMutex,
  StoppableChannel, ThreadPool,
};

use super::{InsertRecord, Op, Record, RotateWriter, WAL_PAGE_SIZE};

pub struct WAL {
  core: Mutex<Core>,
}

impl WAL {
  pub fn open<P>(
    path: P,
    max_file_size: usize,
    max_buffer_size: usize,
    disk: Arc<PageSeeker>,
    checkpoint_timeout: Duration,
  ) -> Result<Self>
  where
    P: AsRef<Path>,
  {
    let wal_disk = PageSeeker::open(path)?;
    let writer = RotateWriter::new(wal_disk, max_file_size);

    Ok(Self {
      core: Mutex::new(Core::new(
        Arc::new(Mutex::new(writer)),
        max_buffer_size,
        disk,
        checkpoint_timeout,
      )),
    })
  }

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

  pub fn commit(&self, tx_id: usize) -> Result<()> {
    self.core.l().commit(tx_id)
  }

  pub fn replay(&self) -> Result<()> {
    self.core.l().replay()
  }
}

struct Core {
  wal_disk: Arc<Mutex<RotateWriter>>,
  last_index: usize,
  last_transaction: usize,
  max_buffer_size: usize,
  checkpoint_c: StoppableChannel<()>,
  background: ThreadPool<Result<()>>,
  source: Arc<PageSeeker>,
}
impl Core {
  fn new(
    wal_disk: Arc<Mutex<RotateWriter>>,
    max_buffer_size: usize,
    source: Arc<PageSeeker>,
    timeout: Duration,
  ) -> Self {
    let (checkpoint_c, rx) = StoppableChannel::new();
    let core = Self {
      wal_disk,
      last_index: 0,
      last_transaction: 0,
      max_buffer_size: max_buffer_size / WAL_PAGE_SIZE,
      checkpoint_c,
      background: ThreadPool::new(2, max_buffer_size * 6 / 5, "wal", None),
      source,
    };
    core.start_background(rx, timeout);
    return core;
  }

  fn start_background(&self, rx: ContextReceiver<()>, timeout: Duration) {
    let wal_disk = self.wal_disk.clone();
    let disk = self.source.clone();
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
        disk.fsync()?;

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
    let mut wal_disk = self.wal_disk.l();
    wal_disk.append(record)?;
    self.last_index = index;
    self.last_transaction = tx_id;
    if wal_disk.buffered_count() >= self.max_buffer_size {
      self.checkpoint_c.send(());
    }
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

    let mut wal_disk = self.wal_disk.l();
    wal_disk.append(record)?;
    self.last_index = index;
    if wal_disk.buffered_count() >= self.max_buffer_size {
      self.checkpoint_c.send(());
    }
    return Ok(());
  }

  fn commit(&mut self, transaction_id: usize) -> Result<()> {
    let index = self.last_index + 1;
    let record = Record::new(transaction_id, index, Op::Commit);
    let mut wal_disk = self.wal_disk.l();
    wal_disk.append(record)?;
    self.last_index = index;
    if wal_disk.buffered_count() >= self.max_buffer_size {
      self.checkpoint_c.send(());
    }
    return Ok(());
  }

  fn replay(&mut self) -> Result<()> {
    let mut unwritten = BTreeMap::<usize, Record>::new();
    let entries = self.wal_disk.l().read_all()?;
    if entries.len() == 0 {
      logger::info(format!("nothing to replay..."));
      return Ok(());
    }

    for entry in entries {
      for record in entry.iter() {
        self.last_index = self.last_index.max(record.index);
        self.last_transaction =
          self.last_transaction.max(record.transaction_id);
        if let Op::Checkpoint(i) = &record.operation {
          unwritten = unwritten.split_off(&i);
        };
        unwritten.insert(record.index, record.clone());
      }
    }

    let mut redo = BTreeMap::<usize, (bool, Vec<InsertRecord>)>::new();

    for record in unwritten.into_values() {
      match record.operation {
        Op::Checkpoint(_) => continue,
        Op::Commit => {
          if let Some((b, _)) = redo.get_mut(&record.transaction_id) {
            *b = true;
          }
        }
        Op::Insert(r) => {
          if let Some((_, v)) = redo.get_mut(&record.transaction_id) {
            v.push(r);
          }
        }
        Op::Start => {
          redo.insert(record.transaction_id, (false, vec![]));
        }
      }
    }
    if redo.len() == 0 {
      logger::info(format!("no records to redo"));
      return Ok(());
    }
    logger::info(format!("{} records redo start", redo.len()));

    for (committed, records) in redo.into_values() {
      for record in records {
        let page = committed.then(|| record.after).unwrap_or(record.before);
        self.source.write(record.index, page)?;
      }
    }

    let checkpoint = Record::new(0, 0, Op::Checkpoint(self.last_index));
    self.wal_disk.l().append(checkpoint)?;
    self.source.fsync()
  }
}
impl Drop for Core {
  fn drop(&mut self) {
    self.checkpoint_c.send(());
    self.checkpoint_c.terminate();
  }
}
