use std::{
  sync::Arc,
  thread::{current, park, Thread},
};

use crossbeam::atomic::AtomicCell;

use crate::utils::ToArc;

pub fn oneshot<T>() -> (Oneshot<T>, OneshotFulfill<T>) {
  let value = AtomicCell::new(None).to_arc();
  let caller = AtomicCell::new(None).to_arc();
  (
    Oneshot {
      value: value.clone(),
      caller: caller.clone(),
    },
    OneshotFulfill { value, caller },
  )
}

pub struct Oneshot<T> {
  value: Arc<AtomicCell<Option<T>>>,
  caller: Arc<AtomicCell<Option<Thread>>>,
}
impl<T> Oneshot<T> {
  pub fn wait(&self) -> T {
    self.caller.store(Some(current()));
    loop {
      match self.value.take() {
        Some(v) => return v,
        None => park(),
      }
    }
  }
}

pub struct OneshotFulfill<T> {
  value: Arc<AtomicCell<Option<T>>>,
  caller: Arc<AtomicCell<Option<Thread>>>,
}
impl<T> OneshotFulfill<T> {
  pub fn fulfill(&self, result: T) {
    self.value.store(Some(result));
    if let Some(th) = self.caller.take() {
      th.unpark();
    }
  }
}
impl<T> Clone for OneshotFulfill<T> {
  fn clone(&self) -> Self {
    Self {
      value: self.value.clone(),
      caller: self.caller.clone(),
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
