use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  time::Duration,
};

use crate::Result;

use super::{
  BackgroundThread, EagerBufferingThread, IntervalWorkThread, LazyBufferingThread,
  SharedWorkThread, SingleFn, StealingWorkThread, WorkInput,
};

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
  pub fn stealing(self, count: usize) -> StealingWorkBuilder {
    StealingWorkBuilder {
      builder: self,
      count,
    }
  }
}

pub struct SharedWorkBuilder {
  builder: WorkBuilder,
  count: usize,
}
impl SharedWorkBuilder {
  // pub fn build<T, R, F, E, W>(
  //   self,
  //   build: F,
  // ) -> std::result::Result<SharedWorkThread<T, R>, E>
  // where
  //   T: Send + UnwindSafe + 'static,
  //   R: Send + 'static,
  //   W: Fn(T) -> R + RefUnwindSafe + Send + Sync + 'static,
  //   F: Fn(usize) -> std::result::Result<W, E>,
  // {
  //   SharedWorkThread::build(
  //     self.builder.name,
  //     self.builder.stack_size,
  //     self.count,
  //     build,
  //   )
  // }

  pub fn build<T, R, F>(self, build: F) -> SharedWorkThread<T, R>
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

pub struct StealingWorkBuilder {
  builder: WorkBuilder,
  count: usize,
}
impl StealingWorkBuilder {
  pub fn build<T, R, F>(self, build: F) -> impl BackgroundThread<T, R>
  where
    T: Send + UnwindSafe + 'static,
    R: Send + 'static,
    F: Fn(T) -> R + RefUnwindSafe + Send + Sync + 'static,
  {
    StealingWorkThread::new(
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
  pub fn interval<T, R, F>(self, timeout: Duration, f: F) -> impl BackgroundThread<T, R>
  where
    T: Send + UnwindSafe + RefUnwindSafe + 'static,
    R: Send + 'static,
    F: FnMut(Option<T>) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    IntervalWorkThread::new(
      self.builder.name,
      self.builder.stack_size,
      timeout,
      SingleFn::new(f),
    )
  }
  pub fn from_channel<T, R>(self, channel: WorkInput<T, R>) -> FromChannelBuilder<T, R> {
    FromChannelBuilder {
      builder: self.builder,
      input: channel,
    }
  }

  pub fn eager_buffering<A, B, C, T, R>(
    self,
    count: usize,
    when_buffered: A,
    result: B,
  ) -> impl BackgroundThread<T, R>
  where
    T: Send + UnwindSafe + 'static,
    R: Send + 'static,
    C: Send + RefUnwindSafe + Sync + 'static,
    A: FnMut(Vec<T>) -> C + RefUnwindSafe + Send + Sync + 'static,
    B: for<'a> Fn(&'a C) -> R + Send + Sync + RefUnwindSafe + 'static,
  {
    EagerBufferingThread::new(
      self.builder.name,
      self.builder.stack_size,
      count,
      when_buffered,
      result,
    )
  }

  pub fn lazy_buffering<T, R, E, F>(
    self,
    timeout: Duration,
    count: usize,
    when_buffered: F,
    make_result: E,
  ) -> impl BackgroundThread<T, R>
  where
    T: Send + UnwindSafe + RefUnwindSafe + 'static,
    R: Send + 'static,
    F: FnMut(()) -> bool + Send + RefUnwindSafe + Sync + 'static,
    E: FnMut((T, bool)) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    LazyBufferingThread::new(
      self.builder.name,
      self.builder.stack_size,
      count,
      timeout,
      SingleFn::new(when_buffered),
      SingleFn::new(make_result),
    )
  }
}

pub struct FromChannelBuilder<T, R> {
  builder: WorkBuilder,
  input: WorkInput<T, R>,
}
impl<T, R> FromChannelBuilder<T, R> {
  pub fn interval<F>(self, timeout: Duration, f: F) -> Result<impl BackgroundThread<T, R>>
  where
    T: Send + UnwindSafe + RefUnwindSafe + 'static,
    R: Send + 'static,
    F: FnMut(Option<T>) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    IntervalWorkThread::from_channel(
      self.builder.name,
      self.builder.stack_size,
      timeout,
      self.input,
      SingleFn::new(f),
    )
  }
}
