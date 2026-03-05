use std::{
  path::PathBuf,
  sync::atomic::{AtomicUsize, Ordering},
  thread::yield_now,
  time::Duration,
};

use crossbeam::epoch::{self, Atomic, Owned};

use crate::{
  disk::{Page, PagePool, PAGE_SIZE},
  error::Result,
  thread::{SingleWorkInput, SingleWorkThread, WorkBuilder},
  utils::{LogFilter, ToRawPointer},
};

use super::{
  replay, FsyncResult, LogBuffer, LogRecord, ReplayResult, WALSegment, WAL_BLOCK_SIZE,
};

pub struct WALConfig {
  pub base_dir: PathBuf,
  pub prefix: PathBuf,
  pub checkpoint_interval: Duration,
  pub segment_flush_delay: Duration,
  pub segment_flush_count: usize,
  pub group_commit_delay: Duration,
  pub group_commit_count: usize,
  pub max_file_size: usize,
}

pub struct WAL {
  prefix: PathBuf,
  last_log_id: AtomicUsize,
  buffer: Atomic<LogBuffer>,
  max_index: usize,
  page_pool: PagePool<WAL_BLOCK_SIZE>,
  segment_rotate: SingleWorkThread<WALSegment, Result>,
  flush_count: usize,
  flush_interval: Duration,
}
impl WAL {
  pub fn replay(
    config: WALConfig,
    checkpoint: SingleWorkInput<(), Result>,
    logger: LogFilter,
  ) -> Result<(Self, ReplayResult)> {
    let max_index = config.max_file_size / WAL_BLOCK_SIZE;
    let page_pool = PagePool::new(max_index);
    logger.info("start to replay wal segments");

    let replay_result = replay(
      config.base_dir.to_string_lossy().as_ref(),
      config.prefix.to_string_lossy().as_ref(),
      config.group_commit_count,
      config.group_commit_delay,
      &page_pool,
    )?;

    logger.info(format!(
      "wal replay result: last_log_id {} last_tx_id {} aborted {} redo {} segments {}",
      replay_result.last_log_id,
      replay_result.last_tx_id,
      replay_result.aborted.len(),
      replay_result.redo.len(),
      replay_result.segments.len()
    ));

    let prefix = PathBuf::from(config.base_dir).join(config.prefix);

    let segment_rotate = WorkBuilder::new()
      .name("wal checkpoint buffering")
      .stack_size(2 << 20)
      .single()
      .buffering(
        config.segment_flush_delay,
        config.segment_flush_count,
        |(segment, result): (WALSegment, bool)| {
          result.then(|| segment.truncate()).unwrap_or(Ok(()))
        },
        move |_| checkpoint.send(()).wait().is_ok(),
      );

    let buffer = LogBuffer::new(
      page_pool.acquire(),
      0,
      AtomicUsize::new(0).to_raw_ptr(),
      WALSegment::open_new(
        &prefix,
        replay_result.last_log_id,
        config.group_commit_count,
        config.group_commit_delay,
      )?
      .to_raw_ptr(),
    );

    Ok((
      Self {
        last_log_id: AtomicUsize::new(replay_result.last_log_id),
        prefix,
        buffer: Atomic::new(buffer),
        page_pool,
        max_index,
        segment_rotate,
        flush_count: config.group_commit_count,
        flush_interval: config.group_commit_delay,
      },
      replay_result,
    ))
  }

  fn append<F>(&self, create_record: F, flush: bool) -> Result
  where
    F: Fn(usize) -> LogRecord,
  {
    let log_id = self.last_log_id.fetch_add(1, Ordering::Release);
    let record = create_record(log_id).to_bytes_with_len();
    let len = record.len();
    let guard = &epoch::pin();
    let mut fsync: Vec<FsyncResult> = vec![];

    loop {
      let buffer_ptr = self.buffer.load(Ordering::Acquire, guard);
      let buffer = unsafe { &*(buffer_ptr.as_raw()) };

      buffer.pin_segment();
      let (offset, ready) = buffer.pin_entry(len);
      if offset + len < WAL_BLOCK_SIZE {
        buffer.write_at(&record, offset);
        if flush {
          while ready > buffer.load_commit() {
            yield_now();
          }
          buffer.apply_entry_len(ready + 1);
          fsync.push(buffer.flush()?);
        }
        buffer.commit_entry();
        buffer.unpin_segment();

        for f in fsync {
          f.wait()?;
        }
        return Ok(());
      }

      if offset >= WAL_BLOCK_SIZE {
        buffer.unpin_segment();
        yield_now();
        continue;
      }

      let (mut segment, mut pin) = buffer.copy_segment();
      let mut index = buffer.get_index() + 1;
      if index >= self.max_index {
        index = 0;
        segment = WALSegment::open_new(
          &self.prefix,
          log_id,
          self.flush_count,
          self.flush_interval,
        )?
        .to_raw_ptr();
        pin = AtomicUsize::new(0).to_raw_ptr();
      }

      match self.buffer.compare_exchange(
        buffer_ptr,
        Owned::init(LogBuffer::new(
          self.page_pool.acquire(),
          index,
          pin,
          segment,
        )),
        Ordering::Release,
        Ordering::Acquire,
        guard,
      ) {
        Ok(_) => unsafe {
          guard.defer_destroy(buffer_ptr);

          let buffer = &*buffer_ptr.as_raw();
          while ready > buffer.load_commit() {
            yield_now();
          }

          buffer.apply_entry_len(ready);
          buffer.write_to_disk()?;
          if buffer.get_index() + 1 < self.max_index {
            buffer.unpin_segment();
            yield_now();
            continue;
          }
          while buffer.load_segment_pinned() > 1 {
            yield_now();
          }

          let (segment, _) = buffer.take_segement();
          fsync.push(segment.fsync());
          segment.deactive();
          self.segment_rotate.send(segment);
        },
        Err(failed) => unsafe {
          if !buffer.get_index() + 1 < self.max_index {
            (&*failed.current.as_raw()).unpin_segment();
            yield_now();
            continue;
          }
          let (segment, _) = failed.new.take_segement();
          segment.truncate()?;
        },
      }
    }
  }

  pub fn current_log_id(&self) -> usize {
    self.last_log_id.load(Ordering::Acquire)
  }

  pub fn append_insert(
    &self,
    tx_id: usize,
    index: usize,
    page: &Page<PAGE_SIZE>,
  ) -> Result {
    self.append(
      move |log_id| LogRecord::new_insert(log_id, tx_id, index, page.copy()),
      false,
    )
  }
  pub fn checkpoint_and_flush(&self, last_log_id: usize) -> Result {
    self.append(
      move |log_id| LogRecord::new_checkpoint(log_id, last_log_id),
      true,
    )
  }
  pub fn append_start(&self, tx_id: usize) -> Result {
    self.append(move |log_id| LogRecord::new_start(log_id, tx_id), false)
  }
  pub fn commit_and_flush(&self, tx_id: usize) -> Result {
    self.append(|log_id| LogRecord::new_commit(log_id, tx_id), true)
  }
  pub fn append_abort(&self, tx_id: usize) -> Result {
    self.append(|log_id| LogRecord::new_abort(log_id, tx_id), false)
  }

  pub fn twostep_close<'a>(&'a self) -> impl Fn() + 'a {
    self.segment_rotate.close();

    || {
      let guard = &epoch::pin();
      loop {
        unsafe {
          let ptr = self.buffer.load(Ordering::Acquire, guard);
          let buffer = &*ptr.as_raw();
          if buffer.load_offset() >= WAL_BLOCK_SIZE {
            yield_now();
            continue;
          }
          if buffer.load_segment_pinned() > 0 {
            yield_now();
            continue;
          }

          let taken = ptr.into_owned();
          let (segment, _) = taken.take_segement();
          return segment.close();
        }
      }
    }
  }
}
