use std::{
  collections::BTreeSet,
  ops::Mul,
  sync::{Arc, Mutex},
  time::Duration,
};

use crate::{
  buffer::BLOCK_SIZE, disk::Finder, plus_pipe, ContextReceiver, Result, ShortenedMutex,
  StoppableChannel,
};

pub struct FreeList {
  list: Arc<Mutex<BTreeSet<usize>>>,
  file: Arc<Finder<BLOCK_SIZE>>,
  interval: Duration,
  chan: StoppableChannel<(), Result>,
}
impl FreeList {
  pub fn new(interval: Duration, file: Arc<Finder<BLOCK_SIZE>>) -> Self {
    let (chan, rx) = StoppableChannel::new();
    let f = Self {
      list: Default::default(),
      file,
      interval,
      chan,
    };
    f.start_defragmentation(rx)
  }
  pub fn acquire(&self) -> Result<usize> {
    if let Some(i) = self.list.l().pop_first() {
      return Ok(i);
    }

    self.file.len().map(plus_pipe(1))
  }

  pub fn insert(&self, i: usize) {
    self.list.l().insert(i);
  }

  fn start_defragmentation(self, rx: ContextReceiver<(), Result>) -> Self {
    let file = self.file.clone();
    let list = self.list.clone();
    rx.to_new_or_timeout(
      "defragmentation",
      BLOCK_SIZE.mul(2),
      self.interval,
      move |_| {
        let len = file.len()?;
        for i in 0..len {
          let page = file.read(i)?;
          if !page.is_empty() {
            continue;
          }
          list.l().insert(i);
        }
        Ok(())
      },
    );
    self
  }
}
impl Drop for FreeList {
  fn drop(&mut self) {
    self.chan.terminate()
  }
}
