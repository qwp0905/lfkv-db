use std::sync::RwLock;

use utils::ShortRwLocker;

#[allow(unused)]
#[derive(Debug)]
pub struct Counter {
  inner: RwLock<CounterInner>,
}
#[allow(unused)]
impl Counter {
  pub fn new(max: usize) -> Self {
    let inner = RwLock::new(CounterInner::new(max));
    Self { inner }
  }

  pub fn is_overflow(&self) -> bool {
    let inner = self.inner.rl();
    inner.is_overflow()
  }

  pub fn fetch_add(&self) -> usize {
    let mut inner = self.inner.wl();
    inner.add()
  }

  pub fn fetch_sub(&self) -> usize {
    let mut inner = self.inner.wl();
    inner.sub()
  }

  pub fn fetch_max(&self, max: usize) {
    let mut inner = self.inner.wl();
    inner.set_max(max)
  }
}

#[allow(unused)]
#[derive(Debug)]
struct CounterInner {
  max: usize,
  running: usize,
}
impl CounterInner {
  fn new(max: usize) -> Self {
    Self { max, running: 0 }
  }

  fn is_overflow(&self) -> bool {
    self.running >= self.max
  }

  fn add(&mut self) -> usize {
    self.running += 1;
    self.running
  }

  fn sub(&mut self) -> usize {
    self.running -= 1;
    self.running
  }

  fn set_max(&mut self, max: usize) {
    self.max = max;
  }
}
