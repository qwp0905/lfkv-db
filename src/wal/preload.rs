use std::{path::PathBuf, sync::Arc, time::Duration};

use crossbeam::{
  channel::{unbounded, Receiver},
  queue::SegQueue,
};

use crate::{
  thread::{SingleWorkThread, WorkBuilder},
  utils::ToArc,
  wal::WALSegment,
  Result,
};

const SEGMENT_MAX_LIFE: Duration = Duration::from_secs(5);

pub struct SegmentPreload {
  reuse: Arc<SegQueue<WALSegment>>,
  queue: Receiver<Result<WALSegment>>,
  thread: SingleWorkThread<(), Result>,
}
impl SegmentPreload {
  pub fn new(
    prefix: PathBuf,
    generation: usize,
    flush_count: usize,
    flush_interval: Duration,
    max_len: usize,
  ) -> Self {
    let (tx, rx) = unbounded();
    let reuse = SegQueue::<WALSegment>::new().to_arc();
    let reuse_c = reuse.clone();
    let mut generation = generation;
    let thread = WorkBuilder::new()
      .name("wal segment preloader")
      .stack_size(2 << 20)
      .single()
      .with_timeout(SEGMENT_MAX_LIFE, move |trigger| {
        if trigger.is_none() {
          return reuse_c.pop().map(|seg| seg.truncate()).unwrap_or(Ok(()));
        }

        let current = generation;
        generation += 1;

        let segment = reuse_c
          .pop()
          .map(|seg| seg.reuse(&prefix, current).map(|_| seg))
          .unwrap_or_else(|| {
            WALSegment::open_new(&prefix, current, flush_count, flush_interval, max_len)
          })?;

        tx.send(Ok(segment)).unwrap();
        Ok(())
      });

    let _ = thread.send(());
    Self {
      queue: rx,
      thread,
      reuse,
    }
  }

  pub fn load(&self) -> Result<WALSegment> {
    let seg = self.queue.recv().unwrap();
    self.thread.send(()).wait_flatten()?;
    seg
  }

  /**
   * must call after close segment rotate thread
   */
  pub fn close(&self) -> Result {
    self.thread.close();
    while let Ok(result) = self.queue.recv() {
      result.and_then(|seg| seg.truncate())?;
    }
    while let Some(seg) = self.reuse.pop() {
      seg.truncate()?;
    }
    Ok(())
  }

  pub fn reuse(&self, segment: WALSegment) {
    self.reuse.push(segment)
  }
}
unsafe impl Send for SegmentPreload {}
unsafe impl Sync for SegmentPreload {}
