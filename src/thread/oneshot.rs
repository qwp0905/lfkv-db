use std::{
  cell::UnsafeCell,
  mem::MaybeUninit,
  sync::Arc,
  thread::{current, park, Thread},
};

use crossbeam::atomic::AtomicCell;

use crate::{utils::ToArc, Error, Result};

pub fn oneshot<T>() -> (Oneshot<T>, OneshotFulfill<T>) {
  let inner = OneshotInner::new().to_arc();
  (Oneshot(inner.clone()), OneshotFulfill(inner))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
  Waiting,
  Fulfilled,
  Disconnected,
}

struct OneshotInner<T> {
  state: AtomicCell<State>,
  value: UnsafeCell<MaybeUninit<T>>,
  caller: AtomicCell<Option<Thread>>,
}
impl<T> OneshotInner<T> {
  fn new() -> Self {
    Self {
      state: AtomicCell::new(State::Waiting),
      value: UnsafeCell::new(MaybeUninit::uninit()),
      caller: AtomicCell::new(None),
    }
  }
  fn get_value(&self) -> &MaybeUninit<T> {
    unsafe { &*self.value.get() }
  }
  fn get_value_mut(&self) -> &mut MaybeUninit<T> {
    unsafe { &mut *self.value.get() }
  }
}

pub struct Oneshot<T>(Arc<OneshotInner<T>>);
impl<T> Oneshot<T> {
  pub fn wait(self) -> Result<T> {
    self.0.caller.store(Some(current()));
    loop {
      match self
        .0
        .state
        .compare_exchange(State::Fulfilled, State::Disconnected)
        .unwrap_or_else(|s| s)
      {
        State::Fulfilled => return Ok(unsafe { self.0.get_value().assume_init_read() }),
        State::Waiting => park(),
        State::Disconnected => return Err(Error::ChannelDisconnected),
      }
    }
  }
}
impl<T> Oneshot<Result<T>> {
  pub fn wait_result(self) -> Result<T> {
    self.wait()?
  }
}
impl<T> Drop for Oneshot<T> {
  fn drop(&mut self) {
    if let State::Fulfilled = self.0.state.swap(State::Disconnected) {
      unsafe { (&mut *self.0.value.get()).assume_init_drop() };
    }
  }
}

pub struct OneshotFulfill<T>(Arc<OneshotInner<T>>);
impl<T> OneshotFulfill<T> {
  pub fn fulfill(self, result: T) {
    let value = self.0.get_value_mut();
    value.write(result);
    match self
      .0
      .state
      .compare_exchange(State::Waiting, State::Fulfilled)
      .unwrap_or_else(|s| s)
    {
      State::Waiting => {
        self.0.caller.take().map(|th| th.unpark());
      }
      State::Disconnected => unsafe { value.assume_init_drop() },
      State::Fulfilled => unreachable!(),
    }
  }
}
impl<T> Drop for OneshotFulfill<T> {
  fn drop(&mut self) {
    if let Ok(_) = self
      .0
      .state
      .compare_exchange(State::Waiting, State::Disconnected)
    {
      self.0.caller.take().map(|th| th.unpark());
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
    assert_eq!(a.wait().expect("should be fulfilled"), v)
  }

  #[test]
  fn test_fulfill_before_wait() {
    let (rx, tx) = oneshot::<usize>();
    tx.fulfill(42);
    assert_eq!(rx.wait().expect("should be fulfilled"), 42);
  }

  #[test]
  fn test_disconnected_on_fulfill_drop() {
    let (rx, tx) = oneshot::<usize>();
    drop(tx);
    assert!(matches!(rx.wait(), Err(Error::ChannelDisconnected)));
  }

  #[test]
  fn test_disconnected_on_fulfill_drop_async() {
    let (rx, tx) = oneshot::<usize>();
    spawn(move || {
      sleep(Duration::from_millis(100));
      drop(tx);
    });
    assert!(matches!(rx.wait(), Err(Error::ChannelDisconnected)));
  }

  #[test]
  fn test_no_leak_on_rx_drop_after_fulfill() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

    struct Tracked;
    impl Drop for Tracked {
      fn drop(&mut self) {
        DROP_COUNT.fetch_add(1, Ordering::SeqCst);
      }
    }

    DROP_COUNT.store(0, Ordering::SeqCst);
    let (rx, tx) = oneshot::<Tracked>();
    tx.fulfill(Tracked);
    drop(rx);
    assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1);
  }

  #[test]
  fn test_no_leak_on_fulfill_then_wait() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

    struct Tracked;
    impl Drop for Tracked {
      fn drop(&mut self) {
        DROP_COUNT.fetch_add(1, Ordering::SeqCst);
      }
    }

    DROP_COUNT.store(0, Ordering::SeqCst);
    let (rx, tx) = oneshot::<Tracked>();
    tx.fulfill(Tracked);
    let _val = rx.wait().expect("should be fulfilled");
    drop(_val);
    assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1);
  }

  #[test]
  fn test_no_leak_on_fulfill_after_rx_drop() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

    struct Tracked;
    impl Drop for Tracked {
      fn drop(&mut self) {
        DROP_COUNT.fetch_add(1, Ordering::SeqCst);
      }
    }

    DROP_COUNT.store(0, Ordering::SeqCst);
    let (rx, tx) = oneshot::<Tracked>();
    drop(rx);
    tx.fulfill(Tracked);
    assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1);
  }
}
