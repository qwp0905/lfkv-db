use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Arc,
  time::Duration,
};

use crossbeam::channel::{Receiver, RecvTimeoutError};

use super::{Oneshot, OneshotFulfill};
use crate::{
  error::{Error, Result},
  utils::{AsTimer, SafeCallable, SafeCallableMut},
};

pub enum Context<T, R> {
  Work((T, OneshotFulfill<Result<R>>)),
  Term,
}

pub struct SharedFn<T, R>(Arc<dyn Fn(T) -> R + RefUnwindSafe + Send + Sync>);
impl<T, R> SharedFn<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn new(f: Arc<dyn Fn(T) -> R + RefUnwindSafe + Send + Sync>) -> Self {
    Self(f)
  }
  #[inline]
  pub fn call(&self, v: T) -> Result<R> {
    self.0.as_ref().safe_call(v).map_err(Error::Panic)
  }
}

pub struct SingleFn<T, R>(Box<dyn FnMut(T) -> R + RefUnwindSafe + Send + Sync>);
impl<T, R> SingleFn<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn new<F>(f: F) -> Self
  where
    F: FnMut(T) -> R + RefUnwindSafe + Send + Sync + 'static,
  {
    Self(Box::new(f))
  }

  #[inline]
  pub fn call(&mut self, v: T) -> Result<R> {
    self.0.as_mut().safe_call_mut(v).map_err(Error::Panic)
  }
}

pub enum SafeWork<T, R> {
  NoTimeout(SingleFn<T, R>),
  WithTimeout(Duration, SingleFn<Option<T>, R>),
  Buffering(Duration, usize, SingleFn<(T, bool), R>, SingleFn<(), bool>),
}
impl<T, R> SafeWork<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn no_timeout<F>(f: F) -> Self
  where
    F: FnMut(T) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    SafeWork::NoTimeout(SingleFn::new(f))
  }
  pub fn with_timeout<F>(timeout: Duration, f: F) -> Self
  where
    F: FnMut(Option<T>) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    SafeWork::WithTimeout(timeout, SingleFn::new(f))
  }

  pub fn buffering<F, E>(timeout: Duration, count: usize, each: F, before_each: E) -> Self
  where
    F: FnMut((T, bool)) -> R + Send + RefUnwindSafe + Sync + 'static,
    E: FnMut(()) -> bool + Send + RefUnwindSafe + Sync + 'static,
  {
    SafeWork::Buffering(
      timeout,
      count,
      SingleFn::new(each),
      SingleFn::new(before_each),
    )
  }
}
impl<T, R> SafeWork<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn run(&mut self, rx: Receiver<Context<T, R>>) {
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
      SafeWork::Buffering(timeout, count, each, before_each) => {
        let mut buffer = Vec::<(T, OneshotFulfill<Result<R>>)>::with_capacity(*count);
        let mut timer = timeout.as_timer();
        let mut flush = |buffer: &mut Vec<(T, OneshotFulfill<Result<R>>)>| {
          let r = before_each.call(()).unwrap_or(false);
          for (v, done) in buffer.drain(..) {
            done.fulfill(each.call((v, r)));
          }
        };
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
              return flush(&mut buffer);
            }
          }

          if buffer.is_empty() {
            timer.reset();
            continue;
          }

          flush(&mut buffer);
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

pub struct BatchWorkResult<R>(Vec<Oneshot<Result<R>>>);
impl<R> BatchWorkResult<R> {
  pub fn from(v: impl Iterator<Item = Oneshot<Result<R>>>) -> Self {
    Self(v.collect())
  }
  pub fn wait(self) -> Result<Vec<R>> {
    let mut results = Vec::with_capacity(self.0.len());
    for o in self.0 {
      results.push(o.wait()??);
    }
    Ok(results)
  }
}
