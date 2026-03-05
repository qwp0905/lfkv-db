use std::{
  path::PathBuf,
  sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
  thread::yield_now,
  time::Duration,
};

use crossbeam::epoch::{pin, Atomic, Owned};

use crate::{
  disk::{Page, PagePool, PageRef, PAGE_SIZE},
  error::Result,
  thread::{SingleWorkInput, SingleWorkThread, WorkBuilder},
  utils::{LogFilter, ToRawPointer},
};

use super::{replay, FsyncResult, LogRecord, ReplayResult, WALSegment, WAL_BLOCK_SIZE};

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

struct OffsetTicket(AtomicU64);
impl OffsetTicket {
  pub fn new() -> Self {
    Self(AtomicU64::new(2))
  }
  pub fn fetch(&self, len: u32) -> (usize, u32) {
    let prev = self
      .0
      .fetch_add((len as u64) | (1 << 32), Ordering::Release);
    ((prev & 0xFFFF_FFFF) as usize, (prev >> 32) as u32)
  }
  pub fn current(&self) -> usize {
    (self.0.load(Ordering::Acquire) & 0xFFFF_FFFF) as usize
  }
}

struct LogBuffer {
  offset: OffsetTicket,
  entry: PageRef<WAL_BLOCK_SIZE>,
  commit_count: AtomicU32,
  index: usize,
  segment_pin: *mut AtomicUsize,
  segment: *mut WALSegment,
}
impl LogBuffer {
  fn new(
    entry: PageRef<WAL_BLOCK_SIZE>,
    index: usize,
    segment_pin: *mut AtomicUsize,
    segment: *mut WALSegment,
  ) -> Self {
    Self {
      offset: OffsetTicket::new(),
      entry,
      commit_count: AtomicU32::new(0),
      index,
      segment_pin,
      segment,
    }
  }
}
unsafe impl Send for LogBuffer {}
unsafe impl Sync for LogBuffer {}

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
    let guard = &pin();
    let mut fsync: Vec<FsyncResult> = vec![];

    loop {
      let buffer_ptr = self.buffer.load(Ordering::Acquire, guard);
      let buffer = unsafe { &*(buffer_ptr.as_raw()) };

      unsafe { &*buffer.segment_pin }.fetch_add(1, Ordering::Release);
      let (offset, ready) = buffer.offset.fetch(len as u32);
      if offset + len < WAL_BLOCK_SIZE {
        buffer.entry.as_ref().copy_nonoverlapping(&record, offset);
        if flush {
          while ready > buffer.commit_count.load(Ordering::Acquire) {
            yield_now();
          }
          buffer
            .entry
            .as_ref()
            .copy_nonoverlapping(&(((ready + 1) & 0xFFFF) as u16).to_be_bytes(), 0);

          let segment = unsafe { &*buffer.segment };
          segment.write(buffer.index, &buffer.entry)?;
          fsync.push(segment.fsync());
        }
        buffer.commit_count.fetch_add(1, Ordering::Release);
        unsafe { &*buffer.segment_pin }.fetch_sub(1, Ordering::Release);

        for f in fsync {
          f.wait()?;
        }
        return Ok(());
      }

      if offset >= WAL_BLOCK_SIZE {
        unsafe { &*buffer.segment_pin }.fetch_sub(1, Ordering::Release);
        yield_now();
        continue;
      }

      let mut index = buffer.index + 1;
      let (segment, pin, is_new) = if index >= self.max_index {
        index = 0;
        (
          WALSegment::open_new(
            &self.prefix,
            log_id,
            self.flush_count,
            self.flush_interval,
          )?
          .to_raw_ptr(),
          AtomicUsize::new(0).to_raw_ptr(),
          true,
        )
      } else {
        (buffer.segment, buffer.segment_pin, false)
      };

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
          while ready > buffer.commit_count.load(Ordering::Acquire) {
            yield_now();
          }

          buffer
            .entry
            .as_ref()
            .copy_nonoverlapping(&((ready & 0xFFFF) as u16).to_be_bytes(), 0);

          (&*buffer.segment).write(buffer.index, &buffer.entry)?;
          if !is_new {
            yield_now();
            (&*buffer.segment_pin).fetch_sub(1, Ordering::Release);
            continue;
          }

          while (&*buffer.segment_pin).load(Ordering::Acquire) > 1 {
            yield_now();
          }

          let segment = *Box::from_raw(buffer.segment);
          fsync.push(segment.fsync());
          self.segment_rotate.send(segment);
          let _ = Box::from_raw(buffer.segment_pin);
        },
        Err(failed) => unsafe {
          if !is_new {
            yield_now();
            (&*(&*failed.current.as_raw()).segment_pin).fetch_sub(1, Ordering::Release);
            continue;
          }
          let _ = Box::from_raw(failed.new.segment_pin);
          Box::from_raw(failed.new.segment).truncate()?;
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
      let guard = &pin();
      loop {
        unsafe {
          let ptr = self.buffer.load(Ordering::Acquire, guard);
          let buffer = &*ptr.as_raw();
          let offset = buffer.offset.current();
          if offset >= WAL_BLOCK_SIZE {
            yield_now();
            continue;
          }
          if (&*buffer.segment_pin).load(Ordering::Acquire) > 0 {
            yield_now();
            continue;
          }

          let taken = ptr.into_owned();
          Box::from_raw(taken.segment).close();
          let _ = Box::from_raw(taken.segment_pin);
          return;
        }
      }
    }
  }
}
