use std::{
  sync::{Arc, Mutex},
  thread::JoinHandle,
  time::Duration,
};

use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError, Sender};

use crate::{AsTimer, Callable, ShortenedMutex, UnwrappedReceiver, UnwrappedSender};

pub enum BackgroundWork<T, R> {
  NoTimeout(Box<dyn FnMut(T) -> R + Send>),
  WithTimeout(Duration, Box<dyn FnMut(Option<T>) -> R + Send>),
  WithTimer(
    Duration,
    Box<dyn FnMut(Option<(T, Sender<R>)>) -> bool + Send>,
  ),
}
impl<T, R> BackgroundWork<T, R> {
  pub fn no_timeout<F>(f: F) -> Self
  where
    F: FnMut(T) -> R + Send + 'static,
  {
    BackgroundWork::NoTimeout(Box::new(f))
  }

  pub fn with_timeout<F>(timeout: Duration, f: F) -> Self
  where
    F: FnMut(Option<T>) -> R + Send + 'static,
  {
    BackgroundWork::WithTimeout(timeout, Box::new(f))
  }

  pub fn with_timer<F>(timeout: Duration, f: F) -> Self
  where
    F: FnMut(Option<(T, Sender<R>)>) -> bool + Send + 'static,
  {
    BackgroundWork::WithTimer(timeout, Box::new(f))
  }

  fn run(&mut self, rx: Receiver<(T, Sender<R>)>) {
    match self {
      BackgroundWork::NoTimeout(job) => {
        while let Ok((v, done)) = rx.recv() {
          let r = job.call(v);
          done.send(r).ok();
        }
      }
      BackgroundWork::WithTimeout(timeout, job) => loop {
        match rx.recv_timeout(*timeout) {
          Ok((v, done)) => {
            let r = job.call(Some(v));
            done.send(r).ok();
          }
          Err(RecvTimeoutError::Timeout) => {
            job.call(None);
          }
          Err(RecvTimeoutError::Disconnected) => break,
        }
      },
      BackgroundWork::WithTimer(timeout, job) => {
        let mut timer = timeout.as_timer();
        loop {
          let arg = match rx.recv_timeout(timer.get_remain()) {
            Ok((v, done)) => Some((v, done)),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => break,
          };
          match job.call(arg) {
            true => timer.reset(),
            false => timer.check(),
          }
        }
      }
    }
  }
}

pub struct BackgroundThread<T, R> {
  inner: Mutex<Option<(JoinHandle<()>, Sender<(T, Sender<R>)>)>>,
  func: Arc<Mutex<BackgroundWork<T, R>>>,
  name: String,
  size: usize,
}
impl<T, R> BackgroundThread<T, R>
where
  T: Send + 'static,
  R: Send + 'static,
{
  pub fn new(name: &str, size: usize, job: BackgroundWork<T, R>) -> Self {
    Self {
      inner: Mutex::new(None),
      func: Arc::new(Mutex::new(job)),
      name: name.to_string(),
      size,
    }
  }

  fn checked_send(&self, v: T) -> Receiver<R> {
    let mut inner = self.inner.l();
    if let Some((t, tx)) = inner.take() {
      if !t.is_finished() {
        let (done_t, done_r) = unbounded();
        tx.must_send((v, done_t));
        *inner = Some((t, tx));
        return done_r;
      }
      drop(tx);
      t.join().ok();
    }

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
    let (done_t, done_r) = unbounded();
    tx.must_send((v, done_t));
    *inner = Some((t, tx));
    return done_r;
  }

  pub fn send(&self, v: T) {
    self.checked_send(v);
  }

  pub fn send_await(&self, v: T) -> R {
    self.checked_send(v).must_recv()
  }

  pub fn close(&self) {
    self.inner.l().take().map(|(t, tx)| {
      if !t.is_finished() {
        drop(tx);
      }
      t.join().ok();
    });
  }
}
