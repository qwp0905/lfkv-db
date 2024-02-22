use std::thread::{Builder, JoinHandle};

use crate::{logger, ContextReceiver, StoppableChannel};

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
  channel: StoppableChannel<T, R>,
  name: String,
  thread: Option<JoinHandle<()>>,
}

impl<T, R, const N: usize> BackgroundThread<T, R, N>
where
  T: Send + 'static,
  R: Send + 'static,
{
  pub fn new<F>(name: &str, job: F) -> Self
  where
    F: FnMut(ContextReceiver<T, R>) + Send + 'static,
  {
    let (channel, thread) = generate(name.to_string(), N, Box::new(job));
    Self {
      channel,
      name: name.to_string(),
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

  pub fn send(&self, v: T) {
    // self.ensure_thread();
    self.channel.send(v);
  }

  pub fn send_await(&self, v: T) -> R {
    // self.ensure_thread();
    self.channel.send_await(v)
  }

  pub fn get_channel(&self) -> StoppableChannel<T, R> {
    self.channel.clone()
  }
}

impl<T, R, const N: usize> Drop for BackgroundThread<T, R, N> {
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
