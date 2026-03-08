use std::{
  path::PathBuf,
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
  },
  time::Duration,
};

use crossbeam::{
  epoch::{self, Atomic, Owned},
  queue::SegQueue,
  utils::Backoff,
};

use crate::{
  disk::{Page, PagePool, PAGE_SIZE},
  error::Result,
  thread::{SingleWorkInput, SingleWorkThread, WorkBuilder},
  utils::{LogFilter, ToArc, ToRawPointer, UnsafeBorrow},
};

use super::{
  replay, FsyncResult, LogBuffer, LogRecord, ReplayResult, SegmentPreload, WALSegment,
  WAL_BLOCK_SIZE,
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
  preloader: Arc<SegmentPreload>,
  last_log_id: AtomicUsize,
  buffer: Atomic<LogBuffer>,
  max_index: usize,
  page_pool: PagePool<WAL_BLOCK_SIZE>,
  wait_checkpoint: SingleWorkThread<WALSegment, Result>,
  not_flushed: Arc<SegQueue<WALSegment>>,
  fsync_queue: SegQueue<(FsyncResult, Arc<AtomicBool>)>,
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

    let preloader = SegmentPreload::new(
      prefix,
      replay_result.generation,
      config.group_commit_count,
      config.group_commit_delay,
      max_index,
    )
    .to_arc();

    let not_flushed = SegQueue::new().to_arc();
    let wait_checkpoint = WorkBuilder::new()
      .name("wal checkpoint buffering")
      .stack_size(2 << 20)
      .single()
      .buffering(
        config.segment_flush_delay,
        config.segment_flush_count,
        handle_rotate(preloader.clone(), not_flushed.clone()),
        move |_| checkpoint.send(()).wait().is_ok(),
      );

    let buffer = LogBuffer::new(
      page_pool.acquire(),
      0,
      AtomicUsize::new(0).to_raw_ptr(),
      preloader.load()?.to_raw_ptr(),
      AtomicBool::new(true).to_arc(),
    );

    Ok((
      Self {
        last_log_id: AtomicUsize::new(replay_result.last_log_id),
        preloader,
        buffer: Atomic::new(buffer),
        page_pool,
        max_index,
        wait_checkpoint,
        not_flushed,
        fsync_queue: SegQueue::new(),
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
    let backoff = Backoff::new();

    loop {
      let buffer_ptr = self.buffer.load(Ordering::Acquire, guard);
      let buffer = buffer_ptr.as_raw().borrow_unsafe();

      buffer.pin_segment();
      let (offset, ready) = buffer.pin_entry(len);
      if offset + len < WAL_BLOCK_SIZE {
        buffer.write_at(&record, offset);
        if !flush {
          buffer.commit_entry();
          buffer.unpin_segment();
          return Ok(());
        }

        while ready > buffer.load_commit() {
          backoff.snooze();
        }
        buffer.apply_entry_len(ready + 1);

        let f = buffer.flush()?;
        buffer.commit_entry();
        buffer.unpin_segment();

        while !buffer.is_prev_flushed() {
          match self.fsync_queue.pop() {
            Some((result, flushed)) => {
              result.wait()?;
              flushed.store(true, Ordering::Release);
            }
            None => backoff.snooze(),
          }
        }

        return f.wait();
      }

      if offset >= WAL_BLOCK_SIZE {
        buffer.unpin_segment();
        backoff.snooze();
        continue;
      }

      let (mut segment, mut pin, mut prev_flushed) = buffer.copy_segment();
      let mut index = buffer.get_index() + 1;
      if index >= self.max_index {
        index = 0;
        segment = self.preloader.load()?.to_raw_ptr();
        pin = AtomicUsize::new(0).to_raw_ptr();
        prev_flushed = AtomicBool::new(false).to_arc();
      }

      if let Err(failed) = self.buffer.compare_exchange(
        buffer_ptr,
        Owned::init(LogBuffer::new(
          self.page_pool.acquire(),
          index,
          pin,
          segment,
          prev_flushed.clone(),
        )),
        Ordering::Release,
        Ordering::Acquire,
        guard,
      ) {
        if buffer.get_index() + 1 < self.max_index {
          failed.current.as_raw().borrow_unsafe().unpin_segment();
          backoff.snooze();
          continue;
        }
        let (segment, _) = failed.new.take_segement();
        self.preloader.reuse(segment);
        continue;
      }

      unsafe { guard.defer_destroy(buffer_ptr) };

      let buffer = buffer_ptr.as_raw().borrow_unsafe();
      while ready > buffer.load_commit() {
        backoff.snooze();
      }

      buffer.apply_entry_len(ready);
      buffer.write_to_disk()?;
      if buffer.get_index() + 1 < self.max_index {
        buffer.unpin_segment();
        backoff.snooze();
        continue;
      }
      while buffer.load_segment_pinned() > 1 {
        backoff.snooze();
      }

      let (segment, _) = buffer.take_segement();
      self.fsync_queue.push((segment.fsync(), prev_flushed));
      self.wait_checkpoint.send(segment);
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
  pub fn checkpoint_and_flush(&self, last_log_id: usize, min_active: usize) -> Result {
    self.append(
      move |log_id| LogRecord::new_checkpoint(log_id, last_log_id, min_active),
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
    self.wait_checkpoint.close();

    while let Some((r, f)) = self.fsync_queue.pop() {
      let _ = r.wait();
      f.store(true, Ordering::Release);
    }
    while let Some(seg) = self.not_flushed.pop() {
      self.preloader.reuse(seg);
    }

    || {
      let guard = &epoch::pin();
      let backoff = Backoff::new();
      loop {
        let ptr = self.buffer.load(Ordering::Acquire, guard);
        let buffer = ptr.as_raw().borrow_unsafe();
        if buffer.load_offset() >= WAL_BLOCK_SIZE {
          backoff.snooze();
          continue;
        }
        if buffer.load_segment_pinned() > 0 {
          backoff.snooze();
          continue;
        }

        let taken = unsafe { ptr.into_owned() };
        let (segment, _) = taken.take_segement();
        let _ = self.preloader.close();
        return segment.close();
      }
    }
  }

  pub fn reuse(&self, segment: WALSegment) {
    self.preloader.reuse(segment);
  }
}
unsafe impl Send for WAL {}
unsafe impl Sync for WAL {}

fn handle_rotate(
  preloader: Arc<SegmentPreload>,
  not_flushed: Arc<SegQueue<WALSegment>>,
) -> impl Fn((WALSegment, bool)) -> Result {
  move |(segment, result)| {
    match result {
      true => {
        while let Some(buffered) = not_flushed.pop() {
          preloader.reuse(buffered);
        }
        preloader.reuse(segment)
      }
      false => not_flushed.push(segment),
    }
    Ok(())
  }
}
