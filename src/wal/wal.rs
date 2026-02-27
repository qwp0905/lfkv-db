use std::{
  collections::BTreeSet,
  mem::replace,
  path::PathBuf,
  sync::{Arc, Mutex},
  time::Duration,
};

use crossbeam::channel::Sender;

use super::{replay, LogEntry, LogRecord, WALSegment, WAL_BLOCK_SIZE};
use crate::{
  disk::{Page, PagePool, PageRef, PAGE_SIZE},
  error::{Error, Result},
  utils::{logger, ShortenedMutex, ToArc, ToArcMutex, UnwrappedSender},
};

struct WALBuffer {
  last_log_id: usize,
  entry: LogEntry,
  segment: WALSegment,
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
  buffer: Arc<Mutex<WALBuffer>>,
  page_pool: Arc<PagePool<WAL_BLOCK_SIZE>>,
  max_index: usize,
  checkpoint: Sender<WALSegment>,
  flush_count: usize,
  flush_interval: Duration,
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
    let (
      last_index,
      last_log_id,
      last_tx_id,
      last_free,
      aborted,
      redo,
      current_seg,
      segments,
    ) = replay(
      config.base_dir.to_string_lossy().as_ref(),
      config.prefix.to_string_lossy().as_ref(),
      max_index,
      config.group_commit_count,
      config.group_commit_delay,
      page_pool.clone(),
    )?;

    logger::info(format!(
      "wal replay result: last_index {last_index} last_log_id {last_log_id} last_tx_id {last_tx_id} last_free {last_free} aborted {} redo {} segments {}",
      aborted.len(),
      redo.len(),
      segments.len()
    ));

    let prefix = PathBuf::from(config.base_dir).join(config.prefix);
    let buffer = WALBuffer {
      last_log_id,
      entry: LogEntry::new(last_index),
      segment: match current_seg {
        Some(seg) => seg,
        None => WALSegment::open_new(
          &prefix,
          config.group_commit_count,
          config.group_commit_delay,
        )?,
      },
    }
    .to_arc_mutex();

    Ok((
      Self {
        prefix,
        buffer,
        page_pool,
        max_index,
        checkpoint,
        flush_count: config.group_commit_count,
        flush_interval: config.group_commit_delay,
      },
      last_tx_id,
      last_free,
      aborted,
      redo,
      segments,
    ))
  }

  pub fn flush(&self) -> Result<()> {
    {
      let buffer = self.buffer.l();
      let (i, p) = self.entry_to_page(&buffer.entry);
      buffer.segment.write(i, &p)?;
      buffer.segment.fsync()
    }
    .wait()
  }

  pub fn close(&self) {
    self.buffer.l().segment.close()
  }

  #[inline]
  fn append<F>(&self, create_record: F) -> Result
  where
    F: Fn(usize) -> LogRecord,
  {
    let mut buffer = self.buffer.l();
    let log_id = buffer.last_log_id;
    let record = create_record(log_id);
    match buffer.entry.append(&record) {
      Ok(_) => {
        buffer.last_log_id += 1;
        return Ok(());
      }
      Err(Error::EOF) => {}
      Err(err) => return Err(err),
    };

    let (i, p) = self.entry_to_page(&buffer.entry);
    buffer.segment.write(i, &p)?;
    let mut index = buffer.entry.get_index() + 1;
    if index == self.max_index {
      index = 0;
      let new_seg =
        WALSegment::open_new(&self.prefix, self.flush_count, self.flush_interval)?;
      let old = replace(&mut buffer.segment, new_seg);
      self.checkpoint.must_send(old);
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

  fn entry_to_page(&self, buffer: &LogEntry) -> (usize, PageRef<WAL_BLOCK_SIZE>) {
    let mut page = self.page_pool.acquire();
    let _ = page.as_mut().writer().write(buffer.as_ref());
    (buffer.get_index(), page)
  }
}
