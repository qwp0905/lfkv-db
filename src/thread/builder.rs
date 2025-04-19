use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  time::Duration,
};

use crossbeam::channel::Sender;

use crate::Result;

use super::{SafeWork, SafeWorkThread, SharedWorkThread};

pub struct WorkBuilder {
  name: String,
  stack_size: usize,
}
impl WorkBuilder {
  pub fn new() -> Self {
    WorkBuilder {
      name: Default::default(),
      stack_size: 2 << 20,
    }
  }
  pub fn name<S: ToString>(mut self, name: S) -> Self {
    self.name = name.to_string();
    self
  }
  pub fn stack_size(mut self, size: usize) -> Self {
    self.stack_size = size;
    self
  }
  pub fn shared(self, count: usize) -> SharedWorkBuilder {
    SharedWorkBuilder {
      builder: self,
      count,
    }
  }
  pub fn single(self) -> SafeWorkBuilder {
    SafeWorkBuilder { builder: self }
  }
}

pub struct SharedWorkBuilder {
  builder: WorkBuilder,
  count: usize,
}
impl SharedWorkBuilder {
  pub fn build<T, R, F, E>(
    self,
    build: F,
  ) -> std::result::Result<SharedWorkThread<T, R>, E>
  where
    T: Send + UnwindSafe + 'static,
    R: Send + 'static,
    F: Fn(usize) -> std::result::Result<SafeWork<T, R>, E>,
  {
    SharedWorkThread::build(
      self.builder.name,
      self.builder.stack_size,
      self.count,
      build,
    )
  }
}
pub struct SafeWorkBuilder {
  builder: WorkBuilder,
}
impl SafeWorkBuilder {
  pub fn no_timeout<T, R, F>(self, f: F) -> SafeWorkThread<T, R>
  where
    T: Send + UnwindSafe + 'static,
    R: Send + 'static,
    F: Fn(T) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    SafeWorkThread::new(
      self.builder.name,
      self.builder.stack_size,
      SafeWork::no_timeout(f),
    )
  }

  pub fn with_timeout<T, R, F>(self, timeout: Duration, f: F) -> SafeWorkThread<T, R>
  where
    T: Send + UnwindSafe + 'static,
    R: Send + 'static,
    F: Fn(Option<T>) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    SafeWorkThread::new(
      self.builder.name,
      self.builder.stack_size,
      SafeWork::with_timeout(timeout, f),
    )
  }

  pub fn with_timer<T, R, F>(self, timeout: Duration, f: F) -> SafeWorkThread<T, R>
  where
    T: Send + UnwindSafe + 'static,
    R: Send + 'static,
    F: Fn(Option<(T, Sender<Result<R>>)>) -> bool + Send + RefUnwindSafe + Sync + 'static,
  {
    SafeWorkThread::new(
      self.builder.name,
      self.builder.stack_size,
      SafeWork::with_timer(timeout, f),
    )
  }
}
