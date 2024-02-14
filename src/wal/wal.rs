use std::{
  collections::{BTreeMap, BTreeSet},
  mem::take,
  ops::Add,
  path::PathBuf,
  sync::{Arc, RwLock},
  time::{Duration, Instant},
};

use crate::{
  disk::PageSeeker, ContextReceiver, DroppableReceiver, EmptySender, Page, Result,
  Serializable, ShortenedRwLock, StoppableChannel, ThreadPool, UnwrappedReceiver,
};

use super::{CommitInfo, LogBuffer, LogEntry, LogRecord, Operation, WAL_PAGE_SIZE};

#[derive(Debug, Clone)]
pub struct WriteAheadLogConfig {
  path: PathBuf,
  max_buffer_size: usize,
  checkpoint_interval: Duration,
  checkpoint_count: usize,
  group_commit_delay: Duration,
  group_commit_count: usize,
  max_file_size: usize,
}

struct WriteAheadLog {
  buffer: Arc<LogBuffer>,
  commit_c: StoppableChannel<CommitInfo>,
  background: Arc<ThreadPool<Result<()>>>,
  disk: Arc<PageSeeker<WAL_PAGE_SIZE>>,
  io_c: StoppableChannel<Vec<LogRecord>>,
  checkpoint_c: StoppableChannel<()>,
  config: WriteAheadLogConfig,
  flush_c: StoppableChannel<(), usize>,
  last_index: Arc<RwLock<usize>>,
}
impl WriteAheadLog {
  pub fn open(
    config: WriteAheadLogConfig,
    commit_c: StoppableChannel<CommitInfo>,
    flush_c: StoppableChannel<(), usize>,
    background: Arc<ThreadPool<Result<()>>>,
  ) -> Result<Self> {
    let disk = Arc::new(PageSeeker::open(&config.path)?);
    let buffer = Arc::new(LogBuffer::new());

    let (io_c, io_rx) = StoppableChannel::new();
    let (checkpoint_c, checkpoint_rx) = StoppableChannel::new();

    let core = Self::new(
      buffer,
      commit_c,
      background,
      disk,
      io_c,
      checkpoint_c,
      config,
      flush_c,
      Default::default(),
    );

    let (last_transaction, cursor) = core.replay()?;

    core.buffer.initial_state(last_transaction);
    core.start_checkpoint(checkpoint_rx);
    core.start_io(io_rx, cursor);

    Ok(core)
  }

  fn new(
    buffer: Arc<LogBuffer>,
    commit_c: StoppableChannel<CommitInfo>,
    background: Arc<ThreadPool<Result<()>>>,
    disk: Arc<PageSeeker<WAL_PAGE_SIZE>>,
    io_c: StoppableChannel<Vec<LogRecord>>,
    checkpoint_c: StoppableChannel<()>,
    config: WriteAheadLogConfig,
    flush_c: StoppableChannel<(), usize>,
    last_index: Arc<RwLock<usize>>,
  ) -> Self {
    Self {
      buffer,
      commit_c,
      background,
      disk,
      io_c,
      checkpoint_c,
      config,
      flush_c,
      last_index,
    }
  }

  fn start_io(&self, rx: ContextReceiver<Vec<LogRecord>>, mut cursor: usize) {
    let group_commit_delay = self.config.group_commit_delay;
    let group_commit_count = self.config.group_commit_count;
    let max_file_size = self.config.max_file_size;
    let checkpoint_count = self.config.checkpoint_count;
    let disk = self.disk.clone();
    let checkpoint_c = self.checkpoint_c.clone();
    let last_index = self.last_index.clone();
    let commit_c = self.commit_c.clone();

    self.background.schedule(move || {
      let mut current = LogEntry::new();
      let mut start = Instant::now();
      let mut point = group_commit_delay;
      let mut v = vec![];
      let mut counter = 0;
      while let Ok(o) = rx.recv_done_or_timeout(point) {
        if let Some((records, done)) = o {
          v.push(done);
          for mut record in records {
            counter += 1;
            *last_index.wl() += 1;
            record.index = *last_index.rl();
            if let Operation::Commit = record.operation {
              commit_c.send(CommitInfo::new(record.transaction_id, record.index));
            }

            if !current.is_available(&record) {
              let entry = take(&mut current);
              disk.write(cursor, entry.serialize()?)?;
              cursor = cursor.add(1).rem_euclid(max_file_size);
            }
            current.append(record);
          }

          disk.write(cursor, current.serialize()?)?;
          if v.len() < group_commit_count {
            point -= Instant::now().duration_since(start).min(point);
            start = Instant::now();
            continue;
          }
        }
        disk.fsync()?;
        v.drain(..).for_each(|done| done.close());

        if checkpoint_count <= counter {
          checkpoint_c.send(());
          counter = 0;
        }

        point = group_commit_delay;
        start = Instant::now();
      }
      return Ok(());
    });
  }

  fn start_checkpoint(&self, rx: ContextReceiver<()>) {
    let timeout = self.config.checkpoint_interval;
    let flush_c = self.flush_c.clone();
    let io_c = self.io_c.clone();
    self.background.schedule(move || {
      while let Ok(_) = rx.recv_new_or_timeout(timeout) {
        let rx = flush_c.send_with_done(());
        let to_be_apply = rx.must_recv();
        io_c
          .send_with_done(vec![LogRecord::new_checkpoint(to_be_apply)])
          .drop_one();
      }
      return Ok(());
    });
  }

  pub fn append(&self, tx_id: usize, page_index: usize, data: Page) -> Result<()> {
    self.buffer.append(tx_id, page_index, data);
    if self.buffer.len() >= self.config.max_buffer_size {
      self.io_c.send_with_done(self.buffer.flush());
    }
    Ok(())
  }

  pub fn new_transaction(&self) -> Result<(usize, usize)> {
    let tx_id = self.buffer.new_transaction();
    if self.buffer.len() >= self.config.max_buffer_size {
      self.io_c.send_with_done(self.buffer.flush());
    }
    return Ok((tx_id, *self.last_index.rl()));
  }

  pub fn commit(&self, tx_id: usize) -> Result<()> {
    let records = self.buffer.commit(tx_id);
    Ok(self.io_c.send_with_done(records).drop_one())
  }

  fn replay(&self) -> Result<(usize, usize)> {
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
        if record.index < cursor_index {
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

    let mut to_be_flush = vec![];
    let mut to_be_rollback = vec![];

    for (tx_id, log) in inserts.into_values() {
      if committed.contains(&tx_id) {
        to_be_flush.push((tx_id, log.page_index, log.data));
      } else {
        to_be_rollback.push((tx_id, log.page_index))
      }
    }

    // self.flush_c.send_with_done(to_be_flush).drop_one();
    *self.last_index.wl() = last_index;

    Ok((last_transaction, cursor))
  }
}
