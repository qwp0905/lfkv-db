use std::{
  collections::VecDeque,
  mem::replace,
  panic::{catch_unwind, set_hook, UnwindSafe},
  sync::{Once, RwLock},
  thread::{current, Builder, JoinHandle},
  time::Duration,
};

use crossbeam::channel::{unbounded, Receiver, RecvError, Sender};
use utils::{logger, size, RwLocker};

#[allow(unused)]
type Work<E> = dyn FnOnce() -> E + Send + UnwindSafe + 'static;

#[allow(unused)]
enum Message<T> {
  New(T),
  Term,
}

#[allow(unused)]
#[derive(Debug)]
pub struct ThreadPool<T = ()> {
  ready: Box<Receiver<ThreadWorker<T>>>,
  done: Box<Sender<Message<ThreadWorker<T>>>>,
  main: Option<ThreadWorker<std::io::Result<()>>>,
  name: String,
  max_len: usize,
  worker_size: usize,
  count: RwLock<usize>,
  timeout: Option<Duration>,
}

static PANIC_HOOK: Once = Once::new();

#[allow(unused)]
impl<T: 'static> ThreadPool<T> {
  pub fn new(
    max_len: usize,
    worker_size: usize,
    name: &str,
    timeout: Option<Duration>,
  ) -> Self {
    PANIC_HOOK.call_once(|| {
      set_hook(Box::new(|p| {
        let message = current()
          .name()
          .map(|c| format!("at {} thread, {}", c, p.to_string()))
          .unwrap_or(p.to_string());
        logger::error(message);
      }));
    });

    let (ready_s, ready_r) = unbounded();
    let (done_s, done_r) = unbounded::<Message<ThreadWorker<T>>>();

    let mut main =
      ThreadWorker::new(name.to_owned(), size::byte(max_len * 10), None);
    main.execute(move || {
      let ready_s = Box::new(ready_s);
      while let Message::New(mut worker) = done_r.recv().unwrap() {
        let ready_s = ready_s.to_owned();
        Builder::new()
          .name(format!("{}_clear", worker.name))
          .stack_size(size::byte(10))
          .spawn(move || {
            worker.clear();
            ready_s.send(worker).unwrap();
          })?;
      }
      Ok(())
    });

    Self {
      ready: Box::new(ready_r),
      done: Box::new(done_s),
      main: Some(main),
      name: name.to_owned(),
      max_len,
      worker_size,
      count: RwLock::new(0),
      timeout,
    }
  }

  pub fn schedule<F>(&self, f: F)
  where
    F: FnOnce() -> T + Send + UnwindSafe + 'static,
  {
    let mut thread = self.ready.try_recv().unwrap_or_else(|_| {
      let count = { *self.count.rl() };
      self
        .max_len
        .le(&count)
        .then(|| self.ready.recv().unwrap())
        .unwrap_or_else(|| {
          let mut c = self.count.wl();
          *c += 1;
          ThreadWorker::new(
            format!("{}_{}", self.name, *c),
            self.worker_size,
            self.timeout,
          )
        })
    });
    thread.execute(f);
    self.done.send(Message::New(thread)).unwrap();
  }
}
impl<T> Drop for ThreadPool<T> {
  fn drop(&mut self) {
    self.done.send(Message::Term).unwrap();
    if let Some(mut main) = self.main.take() {
      main.clear();
      drop(main);
    }
    self.ready.iter().for_each(drop);
  }
}
impl<T: 'static> Default for ThreadPool<T> {
  fn default() -> Self {
    Self::new(1024, size::mb(2), "default", Some(Duration::from_secs(300)))
  }
}
impl ThreadPool<()> {}

#[allow(unused)]
#[derive(Debug)]
struct ThreadWorker<T> {
  channel: Sender<Message<(Box<Work<T>>, Sender<()>)>>,
  done: VecDeque<Receiver<()>>,
  name: String,
  stack_size: usize,
  thread: Option<JoinHandle<()>>,
  timeout: Option<Duration>,
}

impl<T: 'static> ThreadWorker<T> {
  fn new(name: String, stack_size: usize, timeout: Option<Duration>) -> Self {
    let (thread, tx) = spawn(stack_size, name.to_owned(), timeout);

    Self {
      channel: tx,
      done: VecDeque::new(),
      name,
      stack_size,
      thread: Some(thread),
      timeout,
    }
  }

  fn respawn(&mut self) {
    if let Some(t) = self.thread.as_mut() {
      if !t.is_finished() {
        return;
      }
      let (thread, tx) =
        spawn(self.stack_size, self.name.to_owned(), self.timeout);
      if let Err(_) = replace(t, thread).join() {
        logger::error(format!(
          "error on thread {}... will be respawn..",
          self.name
        ));
      }
      drop(replace(&mut self.channel, tx));
      return;
    }

    let (thread, tx) =
      spawn(self.stack_size, self.name.to_owned(), self.timeout);
    self.thread = Some(thread);
    self.channel = tx;
  }

  fn execute<F>(&mut self, f: F)
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
  fn clear(&mut self) {
    while let Some(done) = self.done.pop_front() {
      done.iter().for_each(drop);
    }
  }
}
impl<T> Drop for ThreadWorker<T> {
  fn drop(&mut self) {
    if let Some(t) = self.thread.take() {
      if !t.is_finished() {
        self.channel.send(Message::Term).unwrap();
      }
      if let Err(_) = t.join() {
        logger::error(format!("error on thread {}", self.name));
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
