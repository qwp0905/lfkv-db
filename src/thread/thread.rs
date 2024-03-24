use std::{
  sync::{Arc, Mutex},
  thread::JoinHandle,
  time::Duration,
};

use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError, Sender};

use crate::{logger, AsTimer, ShortenedMutex, UnwrappedReceiver, UnwrappedSender};

pub trait Callable<T, R> {
  fn call(&mut self, v: T) -> R;
}
impl<T, R, F: FnMut(T) -> R> Callable<T, R> for F {
  fn call(&mut self, v: T) -> R {
    self(v)
  }
}

pub enum BackgroundWork<T, R> {
  NoTimeout(Box<dyn FnMut(T) -> R + Send>),
  WithTimeout(Duration, Box<dyn FnMut(Option<T>) -> R + Send>),
  WithTimer(
    Duration,
    Box<dyn FnMut(Option<(T, Sender<R>)>) -> bool + Send>,
  ),
  Empty,
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
      BackgroundWork::Empty => {}
    }
  }
}

pub struct BackgroundThread<T, R = ()>(Mutex<BackgroundThreadInner<T, R>>);

struct BackgroundThreadInner<T, R> {
  thread: Option<(JoinHandle<()>, Sender<(T, Sender<R>)>)>,
  func: Arc<Mutex<BackgroundWork<T, R>>>,
  name: String,
  size: usize,
}
impl<T, R> BackgroundThread<T, R>
where
  T: Send + 'static,
  R: Send + 'static,
{
  pub fn new<S>(name: S, size: usize, work: BackgroundWork<T, R>) -> Self
  where
    S: ToString,
  {
    Self(Mutex::new(BackgroundThreadInner {
      thread: None,
      func: Arc::new(Mutex::new(work)),
      name: name.to_string(),
      size,
    }))
  }

  pub fn empty<S>(name: S, size: usize) -> Self
  where
    S: ToString,
  {
    Self(Mutex::new(BackgroundThreadInner {
      thread: None,
      func: Arc::new(Mutex::new(BackgroundWork::Empty)),
      name: name.to_string(),
      size,
    }))
  }

  pub fn set_work(&self, work: BackgroundWork<T, R>) {
    let mut inner = self.0.l();
    inner.func = Arc::new(Mutex::new(work));
  }

  fn checked_send(&self, v: T) -> Receiver<R> {
    let mut inner = self.0.l();
    if let Some((t, tx)) = inner.thread.take() {
      if !t.is_finished() {
        let (done_t, done_r) = unbounded();
        tx.must_send((v, done_t));
        inner.thread = Some((t, tx));
        return done_r;
      }
      close_thread(t, tx);
    }

    let func = inner.func.clone();
    let (tx, rx) = unbounded::<(T, Sender<R>)>();
    let t = std::thread::Builder::new()
      .name(inner.name.clone())
      .stack_size(inner.size)
      .spawn(move || {
        let mut f = func.l();
        f.run(rx);
      })
      .unwrap();
    let (done_t, done_r) = unbounded();
    tx.must_send((v, done_t));
    inner.thread = Some((t, tx));
    return done_r;
  }

  pub fn send(&self, v: T) -> Receiver<R> {
    self.checked_send(v)
  }

  pub fn send_await(&self, v: T) -> R {
    self.checked_send(v).must_recv()
  }

  pub fn close(&self) {
    let mut inner = self.0.l();
    inner.thread.take().map(|(t, tx)| close_thread(t, tx));
    logger::info(format!("{} thread done", inner.name))
  }
}

fn close_thread<T>(t: JoinHandle<()>, tx: Sender<T>) {
  drop(tx);
  if let Err(err) = t.join() {
    logger::error(format!("{:?}", err));
  };
}
