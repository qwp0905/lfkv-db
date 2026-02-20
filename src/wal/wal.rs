use std::{
  collections::{BTreeSet, HashMap},
  ops::{Add, Div},
  path::PathBuf,
  sync::{Arc, Mutex},
  time::Duration,
};

use crossbeam::{channel::Sender, queue::ArrayQueue};

use crate::{
  disk::{DiskController, DiskControllerConfig, Page, PagePool, PageRef, PAGE_SIZE},
  thread::{SingleWorkThread, WorkBuilder},
  utils::{ShortenedMutex, ToArc, ToArcMutex},
  wal::{LogEntry, LogRecord, Operation, WAL_BLOCK_SIZE},
  Error, Result,
};

struct WALBuffer {
  last_log_id: usize,
  entry: LogEntry,
}

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
  buffer: Arc<Mutex<WALBuffer>>,
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
    BTreeSet<usize>,
    Vec<(usize, usize, Page)>,
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

    let (last_index, last_log_id, last_tx_id, last_free, aborted, redo) = replay(&disk)?;
    let buffer = WALBuffer {
      last_log_id,
      entry: LogEntry::new(last_index),
    }
    .to_arc_mutex();
    let waits = ArrayQueue::new(config.group_commit_count);
    let disk_cloned = disk.clone();
    let buffer_cloned = buffer.clone();
    let pool_cloned = page_pool.clone();
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

          let (i, p) = entry_to_page(&pool_cloned, &buffer_cloned.l().entry);
          if let Err(_) = disk_cloned.write(i, &p) {
            while let Some(done) = waits.pop() {
              let _ = done.send(Ok(false));
            }
            return true;
          };

          let result = disk_cloned.fsync().is_ok();
          while let Some(done) = waits.pop() {
            let _ = done.send(Ok(result));
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
      last_tx_id,
      last_free,
      aborted,
      redo,
    ))
  }

  pub fn flush(&self) -> Result<()> {
    self
      .flush_th
      .send_await(())?
      .then(|| Ok(()))
      .unwrap_or(Err(Error::FlushFailed))
  }

  #[inline]
  fn append<F>(&self, mut f: F) -> Result
  where
    F: FnMut(usize) -> LogRecord,
  {
    let mut buffer = self.buffer.l();
    let log_id = buffer.last_log_id;
    let record = f(log_id);
    match buffer.entry.append(&record) {
      Ok(_) => {
        buffer.last_log_id += 1;
        return Ok(());
      }
      Err(Error::EOF) => {}
      Err(err) => return Err(err),
    }

    let (i, p) = entry_to_page(&self.page_pool, &buffer.entry);
    self.disk.write(i, &p)?;
    let index = buffer.entry.get_index().add(1);
    if index == self.max_index {
      buffer.entry = LogEntry::new(0);
      return Err(Error::WALCapacityExceeded);
    }

    buffer.entry = LogEntry::new(index);
    let _ = buffer.entry.append(&record);

    buffer.last_log_id += 1;
    Ok(())
  }

  pub fn append_insert(
    &self,
    tx_id: usize,
    index: usize,
    page: &Page<PAGE_SIZE>,
  ) -> Result {
    self.append(move |log_id| LogRecord::new_insert(log_id, tx_id, index, page.copy()))
  }
  pub fn append_checkpoint(&self, last_free: usize) -> Result {
    self.append(move |log_id| LogRecord::new_checkpoint(log_id, last_free))
  }
  pub fn append_start(&self, tx_id: usize) -> Result {
    self.append(move |log_id| LogRecord::new_start(log_id, tx_id))
  }
  pub fn append_free(&self, last_free: usize) -> Result {
    self.append(move |log_id| LogRecord::new_free(log_id, last_free))
  }
  pub fn append_commit(&self, tx_id: usize) -> Result {
    self.append(|log_id| LogRecord::new_commit(log_id, tx_id))
  }
  pub fn append_abort(&self, tx_id: usize) -> Result {
    self.append(|log_id| LogRecord::new_abort(log_id, tx_id))
  }
}

fn replay(
  wal: &DiskController<WAL_BLOCK_SIZE>,
) -> Result<(
  usize,
  usize,
  usize,
  usize,
  BTreeSet<usize>,
  Vec<(usize, usize, Page)>,
)> {
  let len = wal.len()?;
  let mut tx_id = 0;
  let mut log_id = 0;
  let mut index = 0;
  let mut free = Vec::new();

  let mut records = vec![];
  for i in 0..len.div(WAL_BLOCK_SIZE) {
    for record in Vec::<LogRecord>::try_from(wal.read(i)?.as_ref())? {
      records.push((i, record))
    }
  }
  records.sort_by_key(|(_, r)| r.log_id);

  let mut apply = HashMap::<usize, Vec<(usize, usize, Page)>>::new();
  let mut commited = Vec::<(usize, usize, Page)>::new();
  if let Some((i, record)) = records.last() {
    index = *i;
    log_id = record.log_id;
  }

  for (_, record) in records {
    tx_id = tx_id.max(record.tx_id);
    match record.operation {
      Operation::Insert(i, page) => {
        if let Some(f) = free.last() {
          if *f == i {
            free.pop();
          }
        }
        apply
          .entry(record.tx_id)
          .or_default()
          .push((record.log_id, i, page));
      }
      Operation::Start => {
        apply.insert(record.tx_id, vec![]);
      }
      Operation::Commit => {
        apply
          .remove(&record.tx_id)
          .map(|pages| commited.extend(pages));
      }
      Operation::Abort => {
        apply.remove(&record.tx_id);
      }
      Operation::Checkpoint(index) => {
        apply.clear();
        commited.clear();
        free = vec![index];
      }
      Operation::Free(index) => {
        free.push(index);
      }
    };
  }

  commited.sort_by_key(|(i, _, _)| *i);
  let aborted = BTreeSet::from_iter(apply.into_keys());
  Ok((
    index,
    log_id,
    tx_id,
    free.last().map(|i| *i).unwrap_or_default(),
    aborted,
    commited,
  ))
}

fn entry_to_page(
  page_pool: &PagePool<WAL_BLOCK_SIZE>,
  buffer: &LogEntry,
) -> (usize, PageRef<WAL_BLOCK_SIZE>) {
  let mut page = page_pool.acquire();
  let _ = page.as_mut().writer().write(buffer.as_ref());
  (buffer.get_index(), page)
}
