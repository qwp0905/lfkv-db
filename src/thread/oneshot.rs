use std::{
  sync::Arc,
  thread::{current, park, Thread},
};

use crossbeam::atomic::AtomicCell;

use crate::utils::ToArc;

pub fn oneshot<T>() -> (Oneshot<T>, OneshotFulfill<T>) {
  let inner = OneshotInner::new().to_arc();
  (Oneshot(inner.clone()), OneshotFulfill(inner))
}

struct OneshotInner<T> {
  value: AtomicCell<Option<T>>,
  caller: AtomicCell<Option<Thread>>,
}
impl<T> OneshotInner<T> {
  fn new() -> Self {
    Self {
      value: AtomicCell::new(None),
      caller: AtomicCell::new(None),
    }
  }
}

pub struct Oneshot<T>(Arc<OneshotInner<T>>);
impl<T> Oneshot<T> {
  pub fn wait(self) -> T {
    self.0.caller.store(Some(current()));
    loop {
      match self.0.value.take() {
        Some(v) => return v,
        None => park(),
      }
    }
  }
}

pub struct OneshotFulfill<T>(Arc<OneshotInner<T>>);
impl<T> OneshotFulfill<T> {
  pub fn fulfill(self, result: T) {
    self.0.value.store(Some(result));
    if let Some(th) = self.0.caller.take() {
      th.unpark();
    }
  }
}

unsafe impl<T: Send> Sync for OneshotFulfill<T> {}
unsafe impl<T: Send> Send for OneshotFulfill<T> {}

#[cfg(test)]
mod tests {
  use std::{
    thread::{sleep, spawn},
    time::Duration,
  };

  use super::*;

  #[test]
  fn test_fulfill() {
    let (a, b) = oneshot::<usize>();
    let v = 100;

    spawn(move || {
      sleep(Duration::from_millis(100));
      b.fulfill(v);
    });
    assert_eq!(a.wait(), v)
  }
}
