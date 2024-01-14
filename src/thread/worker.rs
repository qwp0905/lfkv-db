use std::{
  collections::VecDeque,
  mem::replace,
  panic::{catch_unwind, UnwindSafe},
  thread::{Builder, JoinHandle},
  time::Duration,
};

use crate::utils::{logger, DroppableReceiver, EmptySender};
use crossbeam::channel::Receiver;

use super::{ContextReceiver, StoppableChannel, StoppableContext};

#[allow(unused)]
type Work<E> = dyn FnOnce() -> E + Send + UnwindSafe + 'static;

#[allow(unused)]
pub enum Message<T> {
  New(T),
  Term,
}

#[allow(unused)]
#[derive(Debug)]
pub struct WorkerConfig {
  pub stack_size: usize,
  pub name: String,
  pub timeout: Option<Duration>,
}

#[allow(unused)]
pub struct ThreadWorker<T> {
  channel: StoppableChannel<Box<Work<T>>>,
  done: VecDeque<Receiver<()>>,
  thread: Option<JoinHandle<()>>,
  config: WorkerConfig,
}

impl<T: 'static> ThreadWorker<T> {
  pub fn new(config: WorkerConfig) -> Self {
    let (thread, channel) =
      spawn(config.stack_size, config.name.to_owned(), config.timeout);

    Self {
      channel,
      done: VecDeque::new(),
      thread: Some(thread),
      config,
    }
  }

  fn respawn(&mut self) {
    if let Some(t) = self.thread.as_mut() {
      if !t.is_finished() {
        return;
      }
      let (thread, channel) = spawn(
        self.config.stack_size,
        self.config.name.to_owned(),
        self.config.timeout,
      );
      if let Err(_) = replace(t, thread).join() {
        logger::error(format!(
          "error on thread {}... will be respawn..",
          self.config.name
        ));
      }
      drop(replace(&mut self.channel, channel));
      return;
    }

    let (thread, tx) = spawn(
      self.config.stack_size,
      self.config.name.to_owned(),
      self.config.timeout,
    );
    self.thread = Some(thread);
    self.channel = tx;
  }

  pub fn execute<F>(&mut self, f: F)
  where
    F: FnOnce() -> T + Send + UnwindSafe + 'static,
  {
    self.respawn();
    let job = Box::new(f);
    let rx = self.channel.send_with_done(job);
    self.done.push_back(rx);
  }

  #[inline]
  pub fn clear(&mut self) {
    while let Some(done) = self.done.pop_front() {
      done.drop_all();
    }
  }

  pub fn get_name(&self) -> &String {
    &self.config.name
  }
}
impl<T> Drop for ThreadWorker<T> {
  fn drop(&mut self) {
    if let Some(t) = self.thread.take() {
      if !t.is_finished() {
        self.channel.terminate();
      }
      if let Err(_) = t.join() {
        logger::error(format!("error on thread {}", self.config.name));
      }
    }
  }
}

fn spawn<T: 'static>(
  stack_size: usize,
  name: String,
  timeout: Option<Duration>,
) -> (JoinHandle<()>, StoppableChannel<Box<Work<T>>>) {
  let (tx, rx) = StoppableChannel::new();
  let thread = Builder::new()
    .stack_size(stack_size)
    .name(name)
    .spawn(handle_thread(rx, timeout))
    .unwrap();
  (thread, tx)
}

fn handle_thread<T: 'static>(
  rx: ContextReceiver<Box<Work<T>>>,
  timeout: Option<Duration>,
) -> impl FnOnce() + Send + 'static {
  move || {
    while let Ok(StoppableContext::WithDone((job, done))) =
      rx.maybe_timeout(timeout)
    {
      catch_unwind(job).ok();
      done.close();
    }
  }
}
