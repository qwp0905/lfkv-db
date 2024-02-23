use std::{
  sync::RwLock,
  thread::{Builder, JoinHandle},
};

use crate::{logger, ContextReceiver, ShortenedRwLock, StoppableChannel};

pub type BackgroundJob<T, R> = dyn CallableBox<ContextReceiver<T, R>> + Send;

pub trait CallableBox<T> {
  fn call_box(self: Box<Self>, v: T);
}
impl<T, F: FnMut(T)> CallableBox<T> for F {
  fn call_box(mut self: Box<Self>, v: T) {
    self(v)
  }
}

pub struct BackgroundThread<T, R, const N: usize> {
  inner: RwLock<BackgroundThreadInner<T, R, N>>,
}

impl<T, R, const N: usize> BackgroundThread<T, R, N>
where
  T: Send + 'static,
  R: Send + 'static,
{
  pub fn new<F>(name: &str, job: F) -> Self
  where
    F: CallableBox<ContextReceiver<T, R>> + Send + 'static,
  {
    Self {
      inner: RwLock::new(BackgroundThreadInner {
        name: name.to_string(),
        thread: None,
        job: Some(Box::new(job)),
      }),
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

  pub fn send(&self, v: T) {
    let mut inner = self.inner.wl();
    if let Some((_, channel)) = inner.thread {
      return channel.send(v);
    }

    let job = inner.job.take().unwrap();
    let (channel, thread) = generate(inner.name.clone(), N, job);
    channel.send(v);
    inner.thread = Some((thread, channel));
  }

  pub fn send_await(&self, v: T) -> R {
    let mut inner = self.inner.wl();
    if let Some((thread, channel)) = inner.thread {
      return channel.send_await(v);
    }

    let job = inner.job.take().unwrap();
    let (channel, thread) = generate(inner.name.clone(), N, job);
    let r = channel.send_await(v);
    inner.thread = Some((thread, channel));
    r
  }

  pub fn get_channel(&self) -> StoppableChannel<T, R> {
    let inner = self.inner.rl();
    let (_, channel) = inner.thread.unwrap();
    channel.clone()
  }
}

impl<T, R, const N: usize> Drop for BackgroundThreadInner<T, R, N> {
  fn drop(&mut self) {
    if let Some((thread, channel)) = self.thread.take() {
      if !thread.is_finished() {
        channel.terminate()
      }

      if let Err(err) = thread.join() {
        logger::error(format!("error on thread {}\n{:?}", self.name, err));
      }
    }
  }
}

pub struct BackgroundThreadInner<T, R, const N: usize> {
  name: String,
  thread: Option<(JoinHandle<()>, StoppableChannel<T, R>)>,
  job: Option<Box<BackgroundJob<T, R>>>,
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
