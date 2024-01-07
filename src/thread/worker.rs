use std::{
  collections::VecDeque,
  mem::replace,
  panic::{catch_unwind, UnwindSafe},
  thread::{Builder, JoinHandle},
  time::Duration,
};

use crossbeam::channel::{unbounded, Receiver, RecvError, Sender};
use utils::logger;

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
#[derive(Debug)]
pub struct ThreadWorker<T> {
  channel: Sender<Message<(Box<Work<T>>, Sender<()>)>>,
  done: VecDeque<Receiver<()>>,
  thread: Option<JoinHandle<()>>,
  config: WorkerConfig,
}

impl<T: 'static> ThreadWorker<T> {
  pub fn new(config: WorkerConfig) -> Self {
    let (thread, tx) =
      spawn(config.stack_size, config.name.to_owned(), config.timeout);

    Self {
      channel: tx,
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
      let (thread, tx) = spawn(
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
      drop(replace(&mut self.channel, tx));
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
    let (tx, rx) = unbounded();
    let job = Box::new(f);
    self.channel.send(Message::New((job, tx))).unwrap();
    self.done.push_back(rx);
  }

  #[inline]
  pub fn clear(&mut self) {
    while let Some(done) = self.done.pop_front() {
      done.iter().for_each(drop);
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
        self.channel.send(Message::Term).unwrap();
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
) -> (JoinHandle<()>, Sender<Message<(Box<Work<T>>, Sender<()>)>>) {
  let (tx, rx) = unbounded::<Message<(Box<Work<T>>, Sender<()>)>>();
  let thread = Builder::new()
    .stack_size(stack_size)
    .name(name)
    .spawn(handle_thread(rx, timeout))
    .unwrap();
  (thread, tx)
}

fn handle_thread<T: 'static>(
  rx: Receiver<Message<(Box<Work<T>>, Sender<()>)>>,
  timeout: Option<Duration>,
) -> impl FnOnce() + Send + 'static {
  move || loop {
    while let Ok(Message::New((job, done))) = timeout
      .map(|to| rx.recv_timeout(to).map_err(|_| RecvError))
      .unwrap_or(rx.recv())
    {
      catch_unwind(job).ok();
      done.send(()).unwrap();
    }
  }
}
