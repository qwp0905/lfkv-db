use std::{
  path::PathBuf,
  sync::atomic::{AtomicUsize, Ordering},
  time::Duration,
};

use crossbeam::channel::{unbounded, Receiver};

use crate::{
  thread::{SingleWorkThread, WorkBuilder},
  wal::WALSegment,
  Result,
};

pub struct SegmentPreload {
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
    let generation = AtomicUsize::new(generation);
    let thread = WorkBuilder::new()
      .name("wal segment preloader")
      .stack_size(2 << 20)
      .single()
      .no_timeout(move |_| {
        let segment = WALSegment::open_new(
          &prefix,
          generation.fetch_add(1, Ordering::Release),
          flush_count,
          flush_interval,
          max_len,
        )?;
        tx.send(Ok(segment)).unwrap();
        Ok(())
      });

    let _ = thread.send(());
    Self { queue: rx, thread }
  }

  pub fn load(&self) -> Result<WALSegment> {
    let seg = self.queue.recv().unwrap();
    let _ = self.thread.send(());
    seg
  }

  pub fn close(&self) -> Result {
    self.thread.close();
    while let Ok(result) = self.queue.recv() {
      result.and_then(|seg| seg.truncate())?;
    }
    Ok(())
  }
}
