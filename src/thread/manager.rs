use std::{
  sync::{Arc, Mutex},
  thread::{Builder, JoinHandle},
};

use crate::{logger, ContextReceiver, ShortenedMutex, StoppableChannel};

pub struct ThreadManager {
  spawner: Arc<Spawner>,
}
impl ThreadManager {
  pub fn new() -> Self {
    Self {
      spawner: Arc::new(Spawner::new()),
    }
  }

  pub fn generate<T, R>(&self) -> (StoppableChannel<T, R>, ContextReceiver<T, R>) {
    StoppableChannel::new(self.spawner.clone())
  }

  pub fn flush(&self) {
    self.spawner.flush()
  }
}

pub struct Spawner(Mutex<Vec<JoinHandle<()>>>);
impl Spawner {
  fn new() -> Self {
    Self(Default::default())
  }

  pub fn spawn<F>(&self, name: &str, size: usize, f: F)
  where
    F: FnOnce() + Send + 'static,
  {
    let s = name.to_string();
    let t = Builder::new()
      .name(s.clone())
      .stack_size(size)
      .spawn(move || {
        let r = f();
        logger::info(format!("{} thread done", s));
        r
      })
      .unwrap();

    self.0.l().push(t)
  }

  fn flush(&self) {
    for t in self.0.l().drain(..) {
      if let Err(err) = t.join() {
        eprintln!("{:?}", err.downcast::<String>());
      };
    }
  }
}
