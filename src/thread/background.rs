use std::{
  sync::Mutex,
  thread::{Builder, JoinHandle},
};

use crate::{
  logger, ContextReceiver, ShortenedMutex, StoppableChannel, UnwrappedReceiver,
};

pub type BackgroundJob<T, R> = dyn Callable<ContextReceiver<T, R>> + Send;

pub trait Callable<T> {
  fn call_box(self: Box<Self>, rx: T);
}
impl<T, F: FnMut(T)> Callable<T> for F {
  fn call_box(mut self: Box<Self>, rx: T) {
    self(rx)
  }
}

pub struct BackgroundThread<T, R, const N: usize>(Mutex<BackgroundThreadInner<T, R, N>>);
impl<T, R, const N: usize> BackgroundThread<T, R, N>
where
  T: Send + 'static,
  R: Send + 'static,
{
  pub fn new<F>(name: &str, job: F) -> Self
  where
    F: FnMut(ContextReceiver<T, R>) + Send + 'static,
  {
    Self(Mutex::new(BackgroundThreadInner::new(
      name.to_string(),
      Box::new(job),
    )))
  }

  pub fn send(&self, v: T) {
    self.0.l().send(v)
  }

  pub fn send_await(&self, v: T) -> R {
    self.0.l().send_await(v)
  }
}

struct BackgroundThreadInner<T, R, const N: usize> {
  channel: StoppableChannel<T, R>,
  name: String,
  thread: Option<JoinHandle<()>>,
}

impl<T, R, const N: usize> BackgroundThreadInner<T, R, N>
where
  T: Send + 'static,
  R: Send + 'static,
{
  fn new(name: String, job: Box<BackgroundJob<T, R>>) -> Self {
    let (channel, thread) = generate(name.clone(), N, job);
    Self {
      channel,
      name,
      thread: Some(thread),
    }
  }

  // fn ensure_thread(&mut self) {
  //   if let Some(thread) = &self.thread {
  //     if !thread.is_finished() {
  //       return;
  //     }
  //   }

  //   let (channel, thread) = generate(self.name.clone(), self.stack_size, self.job);
  //   self.channel = channel;
  //   self.thread = Some(thread);
  // }

  fn send(&mut self, v: T) {
    // self.ensure_thread();
    self.channel.send(v);
  }

  fn send_await(&mut self, v: T) -> R {
    // self.ensure_thread();
    self.channel.send_with_done(v).must_recv()
  }
}

impl<T, R, const N: usize> Drop for BackgroundThreadInner<T, R, N> {
  fn drop(&mut self) {
    if let Some(t) = self.thread.take() {
      if !t.is_finished() {
        self.channel.terminate()
      }

      if let Err(err) = t.join() {
        logger::error(format!("error on thread {}\n{:?}", self.name, err));
      }
    }
  }
}

fn generate<T, R>(
  name: String,
  stack_size: usize,
  job: Box<BackgroundJob<T, R>>,
) -> (StoppableChannel<T, R>, JoinHandle<()>)
where
  T: Send + 'static,
  R: Send + 'static,
{
  let (channel, rx) = StoppableChannel::new();
  let thread = Builder::new()
    .name(name)
    .stack_size(stack_size)
    .spawn(move || job.call_box(rx))
    .unwrap();
  (channel, thread)
}
