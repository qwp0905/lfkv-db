use std::{
  collections::BTreeSet,
  mem::replace,
  path::PathBuf,
  sync::{Arc, Mutex},
  time::Duration,
};

use crossbeam::{channel::Sender, queue::ArrayQueue};

use super::{open_file, replay, LogEntry, LogRecord, WALSegment, WAL_BLOCK_SIZE};
use crate::{
  disk::{DiskController, Page, PagePool, PageRef, PAGE_SIZE},
  error::{Error, Result},
  thread::{OneshotFulfill, SingleWorkThread, WorkBuilder},
  utils::{logger, ShortenedMutex, ToArc, ToArcMutex, UnwrappedSender},
};

struct WALBuffer {
  last_log_id: usize,
  entry: LogEntry,
  disk: DiskController<WAL_BLOCK_SIZE>,
}

pub struct WALConfig {
  pub base_dir: PathBuf,
  pub prefix: PathBuf,
  pub checkpoint_interval: Duration,
  pub group_commit_delay: Duration,
  pub group_commit_count: usize,
  pub max_file_size: usize,
}

pub struct WAL {
  prefix: PathBuf,
  flush_th: SingleWorkThread<(), bool>,
  buffer: Arc<Mutex<WALBuffer>>,
  page_pool: Arc<PagePool<WAL_BLOCK_SIZE>>,
  max_index: usize,
  checkpoint: Sender<WALSegment>,
}
impl WAL {
  pub fn replay(
    config: WALConfig,
    checkpoint: Sender<WALSegment>,
  ) -> Result<(
    Self,
    usize,                     // last tx id
    usize,                     // last free
    BTreeSet<usize>,           // aborted
    Vec<(usize, usize, Page)>, // redo records
    Vec<WALSegment>,           // previous wal segments
  )> {
    let max_index = config.max_file_size / WAL_BLOCK_SIZE;
    let page_pool = PagePool::new(max_index).to_arc();
    let (last_index, last_log_id, last_tx_id, last_free, aborted, redo, disk, segments) =
      replay(
        config.base_dir.to_string_lossy().as_ref(),
        config.prefix.to_string_lossy().as_ref(),
        max_index,
        page_pool.clone(),
      )?;

    logger::info(format!(
      "wal replay result: last_index {last_index} last_log_id {last_log_id} last_tx_id {last_tx_id} last_free {last_free} aborted {} redo {} segments {}",
      aborted.len(),
      redo.len(),
      segments.len()
    ));
    let buffer = WALBuffer {
      last_log_id,
      entry: LogEntry::new(last_index),
      disk,
    }
    .to_arc_mutex();
    let flush_th = WorkBuilder::new()
      .name("wal flush")
      .stack_size(2 << 20)
      .single()
      .with_timer(
        config.group_commit_delay,
        handle_flush(config.group_commit_count, buffer.clone(), page_pool.clone()),
      );

    Ok((
      Self {
        prefix: PathBuf::from(config.base_dir).join(config.prefix),
        buffer,
        page_pool,
        flush_th,
        max_index,
        checkpoint,
      },
      last_tx_id,
      last_free,
      aborted,
      redo,
      segments,
    ))
  }

  pub fn flush(&self) -> Result<()> {
    self
      .flush_th
      .send_await(())?
      .then(|| Ok(()))
      .unwrap_or(Err(Error::FlushFailed))
  }

  pub fn close(&self) {
    self.flush_th.close();
    self.buffer.l().disk.close();
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
    buffer.disk.write(i, &p)?;

    let index = buffer.entry.get_index() + 1;

    if index == self.max_index {
      buffer.entry = LogEntry::new(0);
      let new_segment = open_file(self.prefix.clone(), self.page_pool.clone())?;

      self
        .checkpoint
        .must_send(WALSegment::new(replace(&mut buffer.disk, new_segment)));
    }

    buffer.entry = LogEntry::new(index);
    let _ = buffer.entry.append(&record);

    buffer.last_log_id += 1;
    Ok(())
  }
  pub fn current_log_id(&self) -> usize {
    self.buffer.l().last_log_id
  }

  pub fn append_insert(
    &self,
    tx_id: usize,
    index: usize,
    page: &Page<PAGE_SIZE>,
  ) -> Result {
    self.append(move |log_id| LogRecord::new_insert(log_id, tx_id, index, page.copy()))
  }
  pub fn append_checkpoint(&self, last_free: usize, last_log_id: usize) -> Result {
    self.append(move |log_id| LogRecord::new_checkpoint(log_id, last_free, last_log_id))
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

fn entry_to_page(
  page_pool: &PagePool<WAL_BLOCK_SIZE>,
  buffer: &LogEntry,
) -> (usize, PageRef<WAL_BLOCK_SIZE>) {
  let mut page = page_pool.acquire();
  let _ = page.as_mut().writer().write(buffer.as_ref());
  (buffer.get_index(), page)
}

fn handle_flush(
  count: usize,
  buffer: Arc<Mutex<WALBuffer>>,
  page_pool: Arc<PagePool<WAL_BLOCK_SIZE>>,
) -> impl Fn(Option<((), OneshotFulfill<Result<bool>>)>) -> bool {
  let waits = ArrayQueue::new(count);
  move |v: Option<((), OneshotFulfill<Result<bool>>)>| {
    if let Some((_, done)) = v {
      let _ = waits.push(done);
      if !waits.is_full() {
        return false;
      }
    }

    let buffer = buffer.l();
    let (i, p) = entry_to_page(&page_pool, &buffer.entry);
    if let Err(_) = buffer.disk.write(i, &p) {
      while let Some(done) = waits.pop() {
        done.fulfill(Ok(false));
      }
      return true;
    };

    let result = buffer.disk.fsync().is_ok();
    while let Some(done) = waits.pop() {
      done.fulfill(Ok(result));
    }
    true
  }
}
