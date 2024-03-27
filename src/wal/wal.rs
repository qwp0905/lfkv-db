use std::{
  collections::{BTreeMap, BTreeSet},
  ops::{Add, AddAssign, DivAssign, Mul},
  path::PathBuf,
  sync::{Arc, RwLock},
  time::Duration,
};

use crate::{
  buffer::BufferPool,
  disk::{Finder, FinderConfig},
  logger, size, BackgroundThread, BackgroundWork, Drain, Page, Result, ShortenedRwLock,
};

use super::{CommitInfo, LogBuffer, LogEntry, LogRecord, Operation, WAL_PAGE_SIZE};

#[derive(Debug, Clone)]
pub struct WriteAheadLogConfig {
  pub path: PathBuf,
  pub max_buffer_size: usize,
  pub checkpoint_interval: Duration,
  pub checkpoint_count: usize,
  pub group_commit_delay: Duration,
  pub group_commit_count: usize,
  pub max_file_size: usize,
}

pub struct WriteAheadLog {
  buffer: Arc<LogBuffer>,
  commit_c: Arc<BackgroundThread<CommitInfo, Result>>,
  disk: Arc<Finder<WAL_PAGE_SIZE>>,
  io_c: Arc<BackgroundThread<Vec<LogRecord>, Result>>,
  checkpoint_c: Arc<BackgroundThread<()>>,
  config: WriteAheadLogConfig,
  last_index: Arc<RwLock<usize>>,
}
impl WriteAheadLog {
  pub fn open(
    mut config: WriteAheadLogConfig,
    commit_c: Arc<BackgroundThread<CommitInfo, Result>>,
    flush_c: BackgroundThread<(), Option<usize>>,
    buffer_pool: &Arc<BufferPool>,
  ) -> Result<Self> {
    config.max_file_size.div_assign(WAL_PAGE_SIZE);

    let disk_config = FinderConfig {
      path: config.path.clone(),
      batch_delay: config.group_commit_delay,
      batch_size: config.group_commit_count,
    };
    let disk = Arc::new(Finder::open(disk_config)?);
    let buffer = Arc::new(LogBuffer::new());

    let last_index = Arc::new(RwLock::new(0));

    let io_c = Arc::new(BackgroundThread::empty("wal io", WAL_PAGE_SIZE.mul(1000)));
    let checkpoint_c = Arc::new(BackgroundThread::empty("wal checkpoint", size::kb(2)));

    let core = Self::new(
      buffer,
      commit_c,
      disk,
      io_c,
      checkpoint_c,
      config,
      last_index,
    );

    let (last_transaction, cursor) = core.replay(buffer_pool)?;

    core.buffer.initial_state(last_transaction);
    Ok(core.start_checkpoint(flush_c).start_io(cursor))
  }

  fn new(
    buffer: Arc<LogBuffer>,
    commit_c: Arc<BackgroundThread<CommitInfo, Result>>,
    disk: Arc<Finder<WAL_PAGE_SIZE>>,
    io_c: Arc<BackgroundThread<Vec<LogRecord>, Result>>,
    checkpoint_c: Arc<BackgroundThread<()>>,
    config: WriteAheadLogConfig,
    last_index: Arc<RwLock<usize>>,
  ) -> Self {
    Self {
      buffer,
      commit_c,
      disk,
      io_c,
      checkpoint_c,
      config,
      last_index,
    }
  }

  fn start_io(self, mut cursor: usize) -> Self {
    let max_file_size = self.config.max_file_size;
    let checkpoint_count = self.config.checkpoint_count;
    let disk = self.disk.clone();
    let checkpoint_c = self.checkpoint_c.clone();
    let last_index = self.last_index.clone();
    let commit_c = self.commit_c.clone();
    let mut current = LogEntry::new();
    let mut counter = 0;

    self.io_c.set_work(BackgroundWork::no_timeout(
      move |records: Vec<LogRecord>| {
        counter += records.len();
        for mut record in records {
          let mut l = last_index.wl();
          record.assign_id(l.add(1));
          if let Operation::Commit = record.operation {
            commit_c.send(CommitInfo::new(record.transaction_id, record.index));
          }

          if !current.is_available(&record) {
            let entry = current.drain();
            disk.batch_write_from(cursor, &entry)?;
            cursor = cursor.add(1).rem_euclid(max_file_size);
          }
          current.append(record);
          l.add_assign(1);
        }

        disk.batch_write_from(cursor, &current)?;

        if checkpoint_count.lt(&counter) {
          checkpoint_c.send(());
          counter = 0;
        }
        Ok(())
      },
    ));
    self
  }

  fn start_checkpoint(self, flush_c: BackgroundThread<(), Option<usize>>) -> Self {
    let io_c = self.io_c.clone();
    self.checkpoint_c.set_work(BackgroundWork::with_timeout(
      self.config.checkpoint_interval,
      move |_| {
        if let Some(to_be_apply) = flush_c.send_await(()) {
          io_c
            .send_await(vec![LogRecord::new_checkpoint(to_be_apply)])
            .ok();
        }
      },
    ));
    self
  }

  pub fn append(&self, tx_id: usize, page_index: usize, data: Page) -> Result<()> {
    self.buffer.append(tx_id, page_index, data);
    if self.buffer.len().ge(&self.config.max_buffer_size) {
      self.io_c.send_await(self.buffer.flush())?;
    }
    Ok(())
  }

  pub fn new_transaction(&self) -> Result<(usize, usize)> {
    let tx_id = self.buffer.new_transaction();
    if self.buffer.len().ge(&self.config.max_buffer_size) {
      self.io_c.send_await(self.buffer.flush())?;
    }
    Ok((tx_id, *self.last_index.rl()))
  }

  pub fn commit(&self, tx_id: usize) -> Result<()> {
    let records = self.buffer.commit(tx_id);
    self.io_c.send_await(records)
  }

  pub fn before_shutdown(&self) {
    self.checkpoint_c.send(());
    self.commit_c.close();
    self.checkpoint_c.close();
    self.io_c.close();
    self.disk.close();
  }

  fn replay(&self, buffer_pool: &Arc<BufferPool>) -> Result<(usize, usize)> {
    let mut cursor = 0;
    let mut records: BTreeMap<usize, LogRecord> = BTreeMap::new();

    let mut cursor_index = 0;
    for index in 0..self.config.max_file_size {
      let entry: LogEntry = match self.disk.read(index) {
        Ok(page) => match page.deserialize() {
          Ok(e) => e,
          Err(_) => continue,
        },
        Err(_) => break,
      };
      for record in entry.records {
        if record.index.lt(&cursor_index) {
          cursor = index;
        }
        cursor_index = record.index;

        records.insert(record.index, record);
      }
    }

    let mut last_index = 0;
    let mut last_transaction = 0;
    let mut committed = BTreeSet::new();
    let mut aborted = BTreeSet::new();
    let mut started = BTreeSet::new();
    let mut inserts = BTreeMap::new();
    for record in records.into_values() {
      last_transaction = record.transaction_id.max(last_transaction);
      last_index = record.index.max(last_index);
      match record.operation {
        Operation::Start => {
          started.insert(record.transaction_id);
        }
        Operation::Commit => {
          started.remove(&record.transaction_id).then(|| {
            committed.insert(record.transaction_id);
          });
        }
        Operation::Abort => {
          started.remove(&record.transaction_id).then(|| {
            aborted.insert(record.transaction_id);
          });
        }
        Operation::Checkpoint(i) => {
          inserts = inserts.split_off(&i);
          started.clear();
          committed.clear();
          aborted.clear();
        }
        Operation::Insert(log) => {
          inserts.insert(record.index, (record.transaction_id, log));
        }
      }
    }

    let mut to_be_rollback = vec![];

    for (tx_id, log) in inserts.into_values() {
      if committed.contains(&tx_id) {
        //TODO error occurs in here
        buffer_pool.insert(tx_id, log.page_index, log.data)?;
      } else {
        to_be_rollback.push((tx_id, log.page_index))
      }
    }

    self.checkpoint_c.send(());
    *self.last_index.wl() = last_index;

    logger::info(format!(
      "wal replay last tx {last_transaction}, cursor {cursor}"
    ));
    Ok((last_transaction, cursor))
  }
}
