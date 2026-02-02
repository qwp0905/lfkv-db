use std::{
  ops::{Add, AddAssign},
  path::PathBuf,
  sync::{Arc, Mutex},
  time::Duration,
};

use crossbeam::{channel::Sender, queue::ArrayQueue};

use crate::{
  disk::{RandomWriteDisk, RandomWriteDiskConfig},
  logger, DrainAll, Page, Result, ShortenedMutex, SingleWorkThread, ToArc, ToArcMutex,
  UnwrappedSender, WorkBuilder,
};

use super::{record::LogRecord, WAL_BLOCK_SIZE};

pub struct WriteAheadLogConfig {
  pub path: PathBuf,
  pub max_buffer_size: usize,
  pub checkpoint_interval: Duration,
  pub checkpoint_count: usize,
  pub group_commit_delay: Duration,
  pub group_commit_count: usize,
  pub max_file_size: usize,
}

struct Indexes {
  last_tx_id: usize,
  last_commit_id: usize,
  last_log_id: usize,
}

pub struct WriteAheadLog {
  io_th: Arc<SingleWorkThread<LogRecord, Result>>,
  checkpoint_th: Arc<SingleWorkThread<usize, Result>>,
  indexes: Arc<Mutex<Indexes>>,
  disk: Arc<RandomWriteDisk<WAL_BLOCK_SIZE>>,
}
impl WriteAheadLog {
  pub fn open(
    config: WriteAheadLogConfig,
    flush_th: Arc<SingleWorkThread<usize, Result>>,
  ) -> Result<Self> {
    let disk = RandomWriteDisk::open(RandomWriteDiskConfig {
      path: config.path,
      read_threads: Some(1),
      write_threads: Some(3),
    })?
    .to_arc();

    let waits = ArrayQueue::new(config.group_commit_count);
    let (indexes, cursor) = replay(disk.clone())?;
    let indexes = indexes.to_arc_mutex();
    let bc = (Vec::new(), cursor).to_arc_mutex();
    let disk_c = disk.clone();
    let io_th = WorkBuilder::new()
      .name("wal io")
      .stack_size(10)
      .single()
      .with_timer(
        config.group_commit_delay,
        move |v: Option<(LogRecord, Sender<Result<Result>>)>| {
          if let Some((record, done)) = v {
            let mut to_be_written = Vec::new();
            {
              let b = record.to_bytes();
              let mut lock = bc.l();
              if lock.0.len().add(b.len()).gt(&WAL_BLOCK_SIZE) {
                to_be_written.push((lock.1, lock.0.drain_all()));
                lock.1 = lock.1.add(1).rem_euclid(config.max_file_size);
              }
              lock.0.extend_from_slice(b.as_ref())
            }

            for (i, buf) in to_be_written {
              if let Err(err) = disk_c.write(i, buf.into()) {
                done.must_send(Err(err));
                return false;
              }
            }

            let lock = bc.l();
            if let Err(err) = disk_c.write(lock.1, lock.0.clone().into()) {
              done.must_send(Err(err));
              return false;
            }

            let _ = waits.push(done);
            if !waits.is_full() {
              return false;
            }
          };

          if waits.is_empty() {
            return true;
          }

          if let Err(_) = disk_c.fsync() {
            return false;
          }

          while let Some(done) = waits.pop() {
            done.must_send(Ok(Ok(())));
          }

          true
        },
      )
      .to_arc();

    let io_c = io_th.clone();
    let indexes_c = indexes.clone();
    let checkpoint_th = WorkBuilder::new()
      .name("wal checkpoint")
      .stack_size(1)
      .single()
      .with_timeout(config.checkpoint_interval, move |v: Option<usize>| {
        if let Some(tx_id) = v {
          if let Err(err) = flush_th.send_await(tx_id) {
            logger::error(format!("{:?}", err))
          }
          let log_id = {
            let mut indexes = indexes_c.l();
            let log_id = indexes.last_log_id;
            indexes.last_log_id.add_assign(1);
            log_id
          };
          return io_c.send_await(LogRecord::new_checkpoint(log_id, tx_id))?;
        }
        Ok(())
      })
      .to_arc();

    Ok(Self {
      io_th,
      checkpoint_th,
      indexes,
      disk,
    })
  }
  pub fn new_transaction(&self) -> Result<(usize, usize)> {
    let (log_id, tx_id, commit_id) = {
      let mut indexes = self.indexes.l();
      let tx_id = indexes.last_tx_id;
      let commit_id = indexes.last_commit_id;
      let log_id = indexes.last_log_id;
      indexes.last_tx_id.add_assign(1);
      indexes.last_log_id.add_assign(1);
      (log_id, tx_id, commit_id)
    };

    let record = LogRecord::new_start(log_id, tx_id);
    self.io_th.send_await(record)??;
    Ok((tx_id, commit_id))
  }

  pub fn append(&self, tx_id: usize, index: usize, data: Page) -> Result<()> {
    let log_id = {
      let mut indexes = self.indexes.l();
      let log_id = indexes.last_log_id;
      indexes.last_log_id.add_assign(1);
      log_id
    };
    let record = LogRecord::new_insert(log_id, tx_id, index, data);
    self.io_th.send_await(record)?
  }

  pub fn commit(&self, tx_id: usize) -> Result<()> {
    let log_id = {
      let mut indexes = self.indexes.l();
      let log_id = indexes.last_log_id;
      indexes.last_log_id.add_assign(1);
      log_id
    };
    let record = LogRecord::new_commit(log_id, tx_id);
    self.io_th.send_await(record)??;
    let mut indexes = self.indexes.l();
    indexes.last_commit_id = tx_id;
    Ok(())
  }
}

fn replay(disk: Arc<RandomWriteDisk<WAL_BLOCK_SIZE>>) -> Result<(Indexes, usize)> {
  if disk.len()?.eq(&0) {
    return Ok((
      Indexes {
        last_tx_id: 0,
        last_commit_id: 0,
        last_log_id: 0,
      },
      0,
    ));
  }
  todo!()
}
