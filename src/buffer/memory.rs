use crossbeam::channel::{unbounded, Receiver, Sender};

use crate::{Error, Page, Result, UnwrappedReceiver, UnwrappedSender};

pub struct MemoryPool<const N: usize> {
  pages: (Sender<Page<N>>, Receiver<Page<N>>),
}

impl<const N: usize> MemoryPool<N> {
  pub fn new(available: usize) -> Self {
    let pages = unbounded();
    (0..available).for_each(|_| pages.0.must_send(Page::new()));
    Self { pages }
  }

  pub fn get(&self) -> Page<N> {
    self.pages.1.must_recv()
  }

  pub fn try_get(&self) -> Result<Page<N>> {
    self.pages.1.try_recv().map_err(|_| Error::MemoryPoolEmpty)
  }

  pub fn release(&self, page: Page<N>) {
    self.pages.0.must_send(page);
  }

  pub fn available(&self) -> usize {
    self.pages.1.len()
  }
}
impl<const N: usize> Drop for MemoryPool<N> {
  fn drop(&mut self) {
    self.pages.1.iter().for_each(drop);
  }
}
