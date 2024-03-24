use std::{
  collections::BTreeSet,
  ops::Mul,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
  },
  time::Duration,
};

use crate::{BackgroundThread, BackgroundWork, Result, ShortenedMutex};

use super::Finder;

pub struct FreeList<const N: usize> {
  list: Arc<Mutex<BTreeSet<usize>>>,
  file: Arc<Finder<N>>,
  chan: BackgroundThread<(), Result>,
  last_index: AtomicUsize,
}
impl<const N: usize> FreeList<N> {
  pub fn new(interval: Duration, file: Arc<Finder<N>>) -> Result<Self> {
    let chan = BackgroundThread::new(
      "defragmentation",
      N.mul(2),
      BackgroundWork::with_timeout(interval, move |_| {
        // let len = file.len()?;
        // for i in 0..len {
        //   if let Err(Error::NotFound) = file.read(i) {
        //     list.l().insert(i);
        //   };
        // }
        Ok(())
      }),
    );
    let last_index = file.len()?;
    Ok(Self {
      list: Default::default(),
      file,
      chan,
      last_index: AtomicUsize::new(last_index),
    })
  }

  pub fn acquire(&self) -> usize {
    if let Some(i) = self.list.l().pop_first() {
      return i;
    }

    self.last_index.fetch_add(1, Ordering::SeqCst)
  }

  pub fn fetch(&self, i: usize) {
    self.last_index.store(i, Ordering::SeqCst)
  }

  pub fn insert(&self, i: usize) {
    self.list.l().insert(i);
  }

  pub fn before_shutdown(&self) {
    self.chan.close();
    self.file.close();
  }
}
