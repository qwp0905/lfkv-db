use std::{
  sync::{Arc, Mutex},
  thread::JoinHandle,
  time::Duration,
};

use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError, Sender};

pub enum Job<T, R> {
  NoTimeout(Box<dyn FnMut(T) -> R + Send>),
  WithTimeout(Duration, Box<dyn FnMut(Option<T>) -> R + Send>),
}
impl<T, R> Job<T, R> {
  fn run(&mut self, rx: Receiver<(T, Sender<R>)>) {
    match self {
      Job::NoTimeout(job) => {
        while let Ok((v, done)) = rx.recv() {
          let r = job(v);
          done.send(r).ok();
        }
      }
      Job::WithTimeout(timeout, job) => loop {
        match rx.recv_timeout(*timeout) {
          Ok((v, done)) => {
            let r = job(Some(v));
            done.send(r).ok();
          }
          Err(RecvTimeoutError::Timeout) => {
            job(None);
          }
          Err(RecvTimeoutError::Disconnected) => break,
        }
      },
    }
  }
}

pub struct Thread<T, R> {
  inner: Option<(JoinHandle<()>, Sender<(T, Sender<R>)>)>,
  func: Arc<Mutex<Job<T, R>>>,
  name: String,
  size: usize,
}
impl<T, R> Thread<T, R>
where
  T: Send + 'static,
  R: Send + 'static,
{
  pub fn new<F>(name: &str, size: usize, f: F) -> Self
  where
    F: FnMut(T) -> R + Send + Sync + 'static,
  {
    Self {
      inner: None,
      func: Arc::new(Mutex::new(Job::NoTimeout(Box::new(f)))),
      name: name.to_string(),
      size,
    }
  }

  fn run(&mut self) {
    let func = self.func.clone();
    let (tx, rx) = unbounded::<(T, Sender<R>)>();
    let t = std::thread::Builder::new()
      .name(self.name.clone())
      .stack_size(self.size)
      .spawn(move || {
        let mut f = func.lock().unwrap();
        f.run(rx);
      })
      .unwrap();
    self.inner = Some((t, tx))
  }

  pub fn close(&mut self) {
    self.inner.take().map(|(t, tx)| {
      if !t.is_finished() {
        drop(tx);
      }
      t.join().ok();
    });
  }
}
