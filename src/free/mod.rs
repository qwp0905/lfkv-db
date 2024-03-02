use std::{
  collections::BTreeSet,
  ops::Mul,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
  },
  time::Duration,
};

use crate::{disk::Finder, ContextReceiver, Result, ShortenedMutex, StoppableChannel};

pub struct FreeList<const N: usize> {
  list: Arc<Mutex<BTreeSet<usize>>>,
  file: Arc<Finder<N>>,
  interval: Duration,
  chan: StoppableChannel<(), Result>,
  last_index: AtomicUsize,
}
impl<const N: usize> FreeList<N> {
  pub fn new(interval: Duration, file: Arc<Finder<N>>) -> Result<Self> {
    let (chan, rx) = StoppableChannel::new();
    let last_index = file.len()?;
    let f = Self {
      list: Default::default(),
      file,
      interval,
      chan,
      last_index: AtomicUsize::new(last_index),
    };
    Ok(f.start_defragmentation(rx))
  }
  pub fn acquire(&self) -> usize {
    if let Some(i) = self.list.l().pop_first() {
      return i;
    }

    self.last_index.fetch_add(1, Ordering::SeqCst)
  }

  pub fn insert(&self, i: usize) {
    self.list.l().insert(i);
  }

  fn start_defragmentation(self, rx: ContextReceiver<(), Result>) -> Self {
    // let file = self.file.clone();
    // let list = self.list.clone();
    rx.to_new_or_timeout("defragmentation", N.mul(2), self.interval, move |_| {
      // let len = file.len()?;
      // for i in 0..len {
      //   if let Err(Error::NotFound) = file.read(i) {
      //     list.l().insert(i);
      //   };
      // }
      Ok(())
    });
    self
  }
}
impl<const N: usize> Drop for FreeList<N> {
  fn drop(&mut self) {
    self.chan.terminate()
  }
}
