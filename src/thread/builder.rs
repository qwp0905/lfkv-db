use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  time::Duration,
};

use crate::Result;

use super::{SafeWork, SharedWorkThread, SingleWorkInput, SingleWorkThread};

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
  pub fn build<T, R, F, E, W>(
    self,
    build: F,
  ) -> std::result::Result<SharedWorkThread<T, R>, E>
  where
    T: Send + UnwindSafe + 'static,
    R: Send + 'static,
    W: Fn(T) -> R + RefUnwindSafe + Send + Sync + 'static,
    F: Fn(usize) -> std::result::Result<W, E>,
  {
    SharedWorkThread::build(
      self.builder.name,
      self.builder.stack_size,
      self.count,
      build,
    )
  }

  pub fn build_unchecked<T, R, F>(self, build: F) -> SharedWorkThread<T, R>
  where
    T: Send + UnwindSafe + 'static,
    R: Send + 'static,
    F: Fn(T) -> R + RefUnwindSafe + Send + Sync + 'static,
  {
    SharedWorkThread::new(
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
  pub fn with_channel<T, R>(
    self,
    input: SingleWorkInput<T, R>,
  ) -> FromChannelBuilder<T, R> {
    FromChannelBuilder {
      input,
      builder: self.builder,
    }
  }

  pub fn with_timeout<T, R, F>(self, timeout: Duration, f: F) -> SingleWorkThread<T, R>
  where
    T: Send + UnwindSafe + 'static,
    R: Send + 'static,
    F: Fn(Option<T>) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    SingleWorkThread::new(
      self.builder.name,
      self.builder.stack_size,
      SafeWork::with_timeout(timeout, f),
    )
  }

  pub fn buffering<T, R, E, F>(
    self,
    timeout: Duration,
    count: usize,
    each: E,
    before_each: F,
  ) -> SingleWorkThread<T, R>
  where
    T: Send + UnwindSafe + 'static,
    R: Send + 'static,
    E: Fn((T, bool)) -> R + Send + RefUnwindSafe + Sync + 'static,
    F: Fn(()) -> bool + Send + RefUnwindSafe + Sync + 'static,
  {
    SingleWorkThread::new(
      self.builder.name,
      self.builder.stack_size,
      SafeWork::buffering(timeout, count, each, before_each),
    )
  }
}

pub struct FromChannelBuilder<T, R> {
  input: SingleWorkInput<T, R>,
  builder: WorkBuilder,
}
impl<T, R> FromChannelBuilder<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn with_timeout<F>(self, timeout: Duration, f: F) -> Result<SingleWorkThread<T, R>>
  where
    F: Fn(Option<T>) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    SingleWorkThread::from_channel(
      self.builder.name,
      self.builder.stack_size,
      SafeWork::with_timeout(timeout, f),
      self.input,
    )
  }
}
