use std::{
  cell::UnsafeCell,
  mem::MaybeUninit,
  panic::UnwindSafe,
  sync::Arc,
  thread::{current, park, Thread},
};

use crossbeam::atomic::AtomicCell;

use crate::{
  utils::{ToArc, UnsafeBorrow, UnsafeBorrowMut},
  Error, Result,
};

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
  caller: UnsafeCell<Option<Thread>>,
}
impl<T> OneshotInner<T> {
  fn new() -> Self {
    Self {
      state: AtomicCell::new(State::Waiting),
      value: UnsafeCell::new(MaybeUninit::uninit()),
      caller: UnsafeCell::new(None),
    }
  }
  #[inline]
  fn get_value(&self) -> &MaybeUninit<T> {
    self.value.get().borrow_unsafe()
  }
  #[inline]
  fn get_value_mut(&self) -> &mut MaybeUninit<T> {
    self.value.get().borrow_mut_unsafe()
  }
  #[inline]
  fn get_caller_mut(&self) -> &mut Option<Thread> {
    self.caller.get().borrow_mut_unsafe()
  }
}

pub struct Oneshot<T>(Arc<OneshotInner<T>>);
impl<T> Oneshot<T> {
  pub fn wait(self) -> Result<T> {
    unsafe { self.0.caller.get().write(Some(current())) };
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
impl<T> Drop for Oneshot<T> {
  fn drop(&mut self) {
    if let State::Fulfilled = self.0.state.swap(State::Disconnected) {
      unsafe { self.0.get_value_mut().assume_init_drop() };
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
        self.0.get_caller_mut().take().map(|th| th.unpark());
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
      self.0.get_caller_mut().take().map(|th| th.unpark());
    }
  }
}

unsafe impl<T: Send> Sync for OneshotFulfill<T> {}
unsafe impl<T: Send> Send for OneshotFulfill<T> {}
impl<T> UnwindSafe for OneshotFulfill<T> {}

#[cfg(test)]
#[path = "tests/oneshot.rs"]
mod tests;
