use std::{
  sync::Mutex,
  thread::{Builder, JoinHandle},
};

use crate::{
  logger, ContextReceiver, ShortenedMutex, StoppableChannel, UnwrappedReceiver,
};

pub type BackgroundJob<T, R> = Box<dyn FnOnce(ContextReceiver<T, R>) + Send>;

pub struct BackgroundThread<T, R>(Mutex<BackgroundThreadInner<T, R>>);
impl<T, R> BackgroundThread<T, R>
where
  T: Send + 'static,
  R: Send + 'static,
{
  pub fn new<F>(name: String, stack_size: usize, job: F) -> Self
  where
    F: FnOnce(ContextReceiver<T, R>) + Send + Sync + 'static,
  {
    Self(Mutex::new(BackgroundThreadInner::new(
      name,
      stack_size,
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

struct BackgroundThreadInner<T, R> {
  job: BackgroundJob<T, R>,
  channel: StoppableChannel<T, R>,
  name: String,
  stack_size: usize,
  thread: Option<JoinHandle<()>>,
}

impl<T, R> BackgroundThreadInner<T, R>
where
  T: Send + 'static,
  R: Send + 'static,
{
  fn new(name: String, stack_size: usize, job: BackgroundJob<T, R>) -> Self {
    let (channel, thread) = generate(name.clone(), stack_size, job);
    Self {
      job,
      channel,
      name,
      stack_size,
      thread: Some(thread),
    }
  }

  fn ensure_thread(&mut self) {
    if let Some(thread) = &self.thread {
      if !thread.is_finished() {
        return;
      }
    }

    let (channel, thread) = generate(self.name.clone(), self.stack_size, self.job);
    self.channel = channel;
    self.thread = Some(thread);
  }

  fn send(&mut self, v: T) {
    self.ensure_thread();
    self.channel.send(v);
  }

  fn send_await(&mut self, v: T) -> R {
    self.ensure_thread();
    self.channel.send_with_done(v).must_recv()
  }
}

impl<T, R> Drop for BackgroundThreadInner<T, R> {
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
  job: BackgroundJob<T, R>,
) -> (StoppableChannel<T, R>, JoinHandle<()>)
where
  T: Send + 'static,
  R: Send + 'static,
{
  let (channel, rx) = StoppableChannel::new();
  let thread = Builder::new()
    .name(name)
    .stack_size(stack_size)
    .spawn(move || job(rx))
    .unwrap();
  (channel, thread)
}
