mod channel;
pub use channel::*;

mod counter;
use counter::*;
mod worker;
use worker::*;

use std::{
  panic::{set_hook, RefUnwindSafe, UnwindSafe},
  sync::{Arc, Once},
  thread::{current, Builder},
  time::Duration,
};

use crossbeam::channel::{unbounded, Receiver};
use utils::{logger, size, DroppableReceiver};

#[allow(unused)]
#[derive(Debug)]
struct BaseConfig {
  name: String,
  stack_size: usize,
  timeout: Option<Duration>,
}
impl BaseConfig {
  fn generate(&self, index: usize) -> WorkerConfig {
    WorkerConfig {
      name: format!("{}_{}", self.name, index),
      stack_size: self.stack_size,
      timeout: self.timeout,
    }
  }
}

#[allow(unused)]
pub struct ThreadPool<T = ()> {
  ready: Box<Receiver<ThreadWorker<T>>>,
  done: Box<StoppableChannel<ThreadWorker<T>>>,
  main: Option<ThreadWorker<std::io::Result<()>>>,
  count: Arc<Counter>,
  config: BaseConfig,
}

static PANIC_HOOK: Once = Once::new();

#[allow(unused)]
impl<T: 'static> ThreadPool<T> {
  pub fn new(
    max_len: usize,
    worker_stack_size: usize,
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

    let (dc, dr) = StoppableChannel::<ThreadWorker<T>>::new();

    let (ready_s, ready_r) = unbounded();
    let (done_s, done_r) = StoppableChannel::<ThreadWorker<T>>::new();

    let count = Arc::new(Counter::new(max_len));
    let mut main = ThreadWorker::new(WorkerConfig {
      name: name.to_owned(),
      stack_size: size::mb(2),
      timeout: None,
    });

    let cc = Arc::clone(&count);
    main.execute(move || {
      let ready_s = Box::new(ready_s);
      while let Ok(mut worker) = done_r.recv_new() {
        let ready_s = ready_s.to_owned();
        let count = Arc::clone(&cc);
        Builder::new()
          .name(format!("{}_clear", worker.get_name()))
          .stack_size(size::byte(20))
          .spawn(move || {
            worker.clear();
            if !count.is_overflow() {
              return ready_s.send(worker).unwrap();
            }
            count.fetch_sub();
          })?;
      }
      Ok(())
    });

    let config = BaseConfig {
      name: name.to_owned(),
      stack_size: worker_stack_size,
      timeout,
    };

    Self {
      ready: Box::new(ready_r),
      done: Box::new(done_s),
      main: Some(main),
      count,
      config,
    }
  }

  pub fn schedule<F>(&self, f: F)
  where
    F: FnOnce() -> T + Send + UnwindSafe + 'static,
  {
    let mut thread = self.ready.try_recv().unwrap_or_else(|_| {
      self
        .count
        .is_overflow()
        .then(|| self.ready.recv().unwrap())
        .unwrap_or_else(|| {
          ThreadWorker::new(self.config.generate(self.count.fetch_add()))
        })
    });
    thread.execute(f);
    self.done.send(thread);
  }

  pub fn scale_out(&self, size: usize) {
    self.count.fetch_max(size);
  }
}
impl<T> Drop for ThreadPool<T> {
  fn drop(&mut self) {
    self.done.terminate();
    if let Some(mut main) = self.main.take() {
      main.clear();
      drop(main);
    }
    self.ready.drop_all();
  }
}
impl<T: 'static> Default for ThreadPool<T> {
  fn default() -> Self {
    Self::new(1024, size::mb(2), "default", Some(Duration::from_secs(300)))
  }
}
impl<T: 'static> UnwindSafe for ThreadPool<T> {}
impl<T: 'static> RefUnwindSafe for ThreadPool<T> {}
