use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Arc,
  time::Duration,
};

use crossbeam::channel::{Receiver, RecvTimeoutError};

use super::{Oneshot, OneshotFulfill};
use crate::{
  error::{Error, Result},
  utils::{AsTimer, SafeCallable, ToArc},
};

pub enum Context<T, R> {
  Work((T, OneshotFulfill<Result<R>>)),
  Term,
}

pub struct SafeFn<T, R>(pub Arc<dyn Fn(T) -> R + RefUnwindSafe + Send + Sync>);
impl<T, R> SafeFn<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn call(&self, v: T) -> Result<R> {
    self.0.as_ref().safe_call(v).map_err(Error::Panic)
  }
}

pub enum SafeWork<T, R> {
  WithTimeout(Duration, SafeFn<Option<T>, R>),
  Buffering(Duration, usize, SafeFn<(T, bool), R>, SafeFn<(), bool>),
}
impl<T, R> SafeWork<T, R> {
  pub fn with_timeout<F>(timeout: Duration, f: F) -> Self
  where
    F: Fn(Option<T>) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    SafeWork::WithTimeout(timeout, SafeFn(f.to_arc()))
  }

  pub fn buffering<F, E>(timeout: Duration, count: usize, each: F, before_each: E) -> Self
  where
    F: Fn((T, bool)) -> R + Send + RefUnwindSafe + Sync + 'static,
    E: Fn(()) -> bool + Send + RefUnwindSafe + Sync + 'static,
  {
    SafeWork::Buffering(
      timeout,
      count,
      SafeFn(each.to_arc()),
      SafeFn(before_each.to_arc()),
    )
  }
}
impl<T, R> SafeWork<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn run(&self, rx: Receiver<Context<T, R>>) {
    match self {
      SafeWork::WithTimeout(timeout, work) => loop {
        match rx.recv_timeout(*timeout) {
          Ok(Context::Work((v, done))) => done.fulfill(work.call(Some(v))),
          Err(RecvTimeoutError::Timeout) => {
            let _ = work.call(None);
          }
          Ok(Context::Term) | Err(RecvTimeoutError::Disconnected) => return,
        };
      },
      SafeWork::Buffering(timeout, count, each, before_each) => {
        let mut buffer = Vec::with_capacity(*count);
        let mut timer = timeout.as_timer();
        loop {
          match rx.recv_timeout(timer.get_remain()) {
            Ok(Context::Work((v, done))) => {
              buffer.push((v, done));
              if buffer.len() < *count {
                timer.check();
                continue;
              }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Ok(Context::Term) | Err(RecvTimeoutError::Disconnected) => {
              let r = before_each.call(()).unwrap_or(false);
              for (v, done) in buffer.drain(..) {
                done.fulfill(each.call((v, r)));
              }
              return;
            }
          }

          if buffer.is_empty() {
            timer.reset();
            continue;
          }

          let r = before_each.call(()).unwrap_or(false);
          for (v, done) in buffer.drain(..) {
            done.fulfill(each.call((v, r)));
          }
          timer.reset();
        }
      }
    }
  }
}

pub struct WorkResult<R>(Oneshot<Result<R>>);
impl<R> WorkResult<R> {
  pub fn wait(self) -> Result<R> {
    self.0.wait()?
  }
}
impl<R> From<Oneshot<Result<R>>> for WorkResult<R> {
  fn from(v: Oneshot<Result<R>>) -> Self {
    WorkResult(v)
  }
}
impl<R> WorkResult<Result<R>> {
  pub fn wait_flatten(self) -> Result<R> {
    self.wait()?
  }
}
