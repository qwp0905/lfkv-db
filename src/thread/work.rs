use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Arc,
  time::Duration,
};

use crossbeam::channel::{Receiver, RecvTimeoutError};

use super::OneshotFulfill;
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
  NoTimeout(SafeFn<T, R>),
  WithTimeout(Duration, SafeFn<Option<T>, R>),
  WithTimer(
    Duration,
    SafeFn<Option<(T, OneshotFulfill<Result<R>>)>, bool>,
  ),
}
impl<T, R> SafeWork<T, R> {
  pub fn no_timeout<F>(f: F) -> Self
  where
    F: Fn(T) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    SafeWork::NoTimeout(SafeFn(f.to_arc()))
  }

  pub fn with_timeout<F>(timeout: Duration, f: F) -> Self
  where
    F: Fn(Option<T>) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    SafeWork::WithTimeout(timeout, SafeFn(f.to_arc()))
  }

  pub fn with_timer<F>(timeout: Duration, f: F) -> Self
  where
    F: Fn(Option<(T, OneshotFulfill<Result<R>>)>) -> bool
      + Send
      + RefUnwindSafe
      + Sync
      + 'static,
  {
    SafeWork::WithTimer(timeout, SafeFn(f.to_arc()))
  }
}
impl<T, R> SafeWork<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn run(&self, rx: Receiver<Context<T, R>>) {
    match self {
      SafeWork::NoTimeout(work) => {
        while let Ok(Context::Work((v, done))) = rx.recv() {
          done.fulfill(work.call(v));
        }
      }
      SafeWork::WithTimeout(timeout, work) => loop {
        match rx.recv_timeout(*timeout) {
          Ok(Context::Work((v, done))) => done.fulfill(work.call(Some(v))),
          Err(RecvTimeoutError::Timeout) => {
            let _ = work.call(None);
          }
          Ok(Context::Term) | Err(RecvTimeoutError::Disconnected) => return,
        };
      },
      SafeWork::WithTimer(duration, work) => {
        let mut timer = duration.as_timer();
        loop {
          match rx.recv_timeout(timer.get_remain()) {
            Ok(Context::Work((v, done))) => {
              match work.call(Some((v, done.clone()))) {
                Ok(true) => timer.reset(),
                Ok(false) => timer.check(),
                Err(err) => done.fulfill(Err(err)),
              };
            }
            Err(RecvTimeoutError::Timeout) => {
              match work.call(None) {
                Ok(true) => timer.reset(),
                Ok(false) => timer.check(),
                Err(_) => {}
              };
            }
            Ok(Context::Term) | Err(RecvTimeoutError::Disconnected) => return,
          };
        }
      }
    }
  }
}
