use std::{
  thread::{Builder, JoinHandle},
  time::Duration,
};

use crossbeam::channel::Sender;

use crate::{
  logger, ContextReceiver, StoppableChannel, UnwrappedReceiver, UnwrappedSender,
};

pub enum BackgroundJob<T, R> {
  New(fn(T) -> R),
  NewOrTimeout(Duration, fn(Option<T>) -> R),
  Done(fn(T) -> R),
  DoneOrTimeout(Duration, fn(Option<(T, Sender<R>)>)),
  All(fn(T) -> R),
}
impl<T, R> BackgroundJob<T, R>
where
  T: Send + 'static,
  R: Send + 'static,
{
  fn to_thread(
    &self,
    name: String,
    stack_size: usize,
    rx: ContextReceiver<T, R>,
  ) -> JoinHandle<()> {
    let builder = Builder::new().name(name).stack_size(stack_size);
    let t = match self {
      BackgroundJob::New(job) => {
        let job = job.clone();
        builder.spawn(move || {
          while let Ok(v) = rx.recv_new() {
            job(v);
          }
        })
      }
      BackgroundJob::NewOrTimeout(timeout, job) => {
        let job = job.clone();
        let timeout = *timeout;
        builder.spawn(move || {
          while let Ok(v) = rx.recv_new_or_timeout(timeout) {
            job(v);
          }
        })
      }
      BackgroundJob::Done(job) => {
        let job = job.clone();
        builder.spawn(move || {
          while let Ok((v, done)) = rx.recv_done() {
            let r = job(v);
            done.must_send(r)
          }
        })
      }
      BackgroundJob::DoneOrTimeout(timeout, job) => {
        let job = job.clone();
        let timeout = *timeout;
        builder.spawn(move || {
          while let Ok(v) = rx.recv_done_or_timeout(timeout) {
            job(v);
          }
        })
      }
      BackgroundJob::All(job) => {
        let job = job.clone();
        builder.spawn(move || {
          while let Ok((v, done)) = rx.recv_all() {
            let r = job(v);
            done.map(|tx| tx.must_send(r));
          }
        })
      }
    };

    t.unwrap()
  }
}

pub struct BackgroundThread<T, R> {
  job: BackgroundJob<T, R>,
  channel: StoppableChannel<T, R>,
  name: String,
  stack_size: usize,
  thread: Option<JoinHandle<()>>,
}

impl<T, R> BackgroundThread<T, R>
where
  T: Send + 'static,
  R: Send + 'static,
{
  pub fn new(name: String, stack_size: usize, job: BackgroundJob<T, R>) -> Self {
    let (channel, rx) = StoppableChannel::new();
    let thread = job.to_thread(name.clone(), stack_size, rx);
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

    let (channel, rx) = StoppableChannel::new();
    let nt = self.job.to_thread(self.name.clone(), self.stack_size, rx);
    self.channel = channel;
    self.thread = Some(nt);
  }

  pub fn send(&self, v: T) {
    self.channel.send(v);
  }

  pub fn send_and_wait(&self, v: T) -> R {
    self.channel.send_with_done(v).must_recv()
  }
}

impl<T, R> Drop for BackgroundThread<T, R> {
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
