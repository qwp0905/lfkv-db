use std::{
  collections::HashMap,
  ops::{Add, Div},
  path::PathBuf,
  sync::{Arc, Mutex},
  time::Duration,
};

use crossbeam::{channel::Sender, queue::ArrayQueue};

use crate::{
  disk::{DiskController, DiskControllerConfig, PagePool, PageRef},
  wal::{
    record::{LogRecord, Operation},
    LogEntry, WAL_BLOCK_SIZE,
  },
  Error, Page, Result, ShortenedMutex, SingleWorkThread, ToArc, ToArcMutex,
  UnwrappedSender, WorkBuilder,
};

pub struct WALConfig {
  pub path: PathBuf,
  pub max_buffer_size: usize,
  pub checkpoint_interval: Duration,
  pub checkpoint_count: usize,
  pub group_commit_delay: Duration,
  pub group_commit_count: usize,
  pub max_file_size: usize,
}

pub struct WAL {
  disk: Arc<DiskController<WAL_BLOCK_SIZE>>,
  flush_th: SingleWorkThread<(), bool>,
  buffer: Arc<Mutex<LogEntry>>,
  page_pool: Arc<PagePool<WAL_BLOCK_SIZE>>,
  max_index: usize,
}
impl WAL {
  pub fn replay(
    config: WALConfig,
  ) -> Result<(
    Self,
    usize,
    usize,
    usize,
    HashMap<usize, Vec<(usize, Page)>>,
  )> {
    let page_pool = PagePool::new(1).to_arc();
    let disk = DiskController::open(
      DiskControllerConfig {
        path: config.path,
        read_threads: Some(1),
        write_threads: Some(3),
      },
      page_pool.clone(),
    )?
    .to_arc();

    let (last_index, last_log_id, last_transaction_id, last_commit_id, redo) =
      replay(&disk)?;
    let buffer = LogEntry::new(last_index).to_arc_mutex();
    let waits = ArrayQueue::new(config.group_commit_count);
    let disk_cloned = disk.clone();
    let flush_th = WorkBuilder::new()
      .name("wal flush")
      .stack_size(1)
      .single()
      .with_timer(
        config.group_commit_delay,
        move |v: Option<((), Sender<Result<bool>>)>| {
          if let Some((_, done)) = v {
            let _ = waits.push(done);
            if !waits.is_full() {
              return false;
            }
          }

          let result = disk_cloned.fsync().is_ok();
          while let Some(done) = waits.pop() {
            done.must_send(Ok(result));
          }
          true
        },
      );
    Ok((
      Self {
        buffer,
        disk,
        page_pool,
        flush_th,
        max_index: config.max_file_size.div(WAL_BLOCK_SIZE),
      },
      last_log_id,
      last_transaction_id,
      last_commit_id,
      redo,
    ))
  }

  fn flush(&self) -> Result<()> {
    self
      .flush_th
      .send_await(())?
      .then(|| Ok(()))
      .unwrap_or(Err(Error::FlushFailed))
  }

  pub fn append(&self, log: LogRecord) -> Result<()> {
    let is_commit = if let Operation::Commit = log.operation {
      true
    } else {
      false
    };
    let page = {
      let mut buffer = self.buffer.l();
      while let Err(Error::EOF) = buffer.append(&log) {
        self.disk.write(&self.entry_to_page(&buffer))?;
        let index = buffer.get_index().add(1);
        *buffer = LogEntry::new(index.eq(&self.max_index).then(|| 0).unwrap_or(index));
      }
      if !is_commit {
        return Ok(());
      }

      self.entry_to_page(&buffer)
    };

    self.disk.write(&page)?;
    self.flush()
  }

  fn entry_to_page(&self, buffer: &LogEntry) -> PageRef<WAL_BLOCK_SIZE> {
    let mut page = self.page_pool.acquire(buffer.get_index());
    page.as_mut().writer().write(buffer.as_ref());
    page
  }
}

fn replay(
  wal: &DiskController<WAL_BLOCK_SIZE>,
) -> Result<(
  usize,
  usize,
  usize,
  usize,
  HashMap<usize, Vec<(usize, Page)>>,
)> {
  let len = wal.len()?;
  let mut tx_id = 0;
  let mut log_id = 0;
  let mut index = 0;
  let mut commit_id = 0;

  let mut apply = HashMap::<usize, Vec<(usize, usize, Page)>>::new();
  let mut commited = HashMap::<usize, Vec<(usize, Page)>>::new();
  for i in 0..len.div(WAL_BLOCK_SIZE) {
    for record in Vec::<LogRecord>::try_from(wal.read(i)?.as_ref())? {
      tx_id = tx_id.max(record.tx_id);
      if log_id < record.log_id {
        index = i;
        log_id = record.log_id;
      }

      match record.operation {
        Operation::Insert(i, page) => {
          apply
            .entry(record.tx_id)
            .or_default()
            .push((record.log_id, i, page));
        }
        Operation::Start => {
          apply.insert(record.tx_id, vec![]);
        }
        Operation::Commit => {
          commit_id = commit_id.max(record.tx_id);
          apply
            .remove(&record.tx_id)
            .map(|mut pages| {
              pages.sort_by_key(|(i, _, _)| *i);
              pages.into_iter().map(|(_, i, p)| (i, p)).collect()
            })
            .and_then(|pages| commited.insert(record.tx_id, pages));
        }
        Operation::Abort => {
          apply.remove(&record.tx_id);
        }
        Operation::Checkpoint => {
          apply.clear();
          commited.clear();
        }
      };
    }
  }

  Ok((index, log_id, tx_id, commit_id, commited))
}
