use std::{
  any::Any,
  fmt::Debug,
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Arc,
  thread::JoinHandle,
  time::Duration,
};

use crossbeam::{
  channel::{unbounded, Receiver, RecvTimeoutError, Sender},
  queue::ArrayQueue,
};

use crate::{logger, AsTimer, UnwrappedReceiver, UnwrappedSender};

pub trait SafeCallable<T, R> {
  type Error;
  fn safe_call(&self, v: T) -> std::result::Result<R, Self::Error>;
}
impl<T, R, F> SafeCallable<T, R> for F
where
  T: UnwindSafe + 'static,
  F: Fn(T) -> R + RefUnwindSafe,
{
  type Error = Box<dyn std::any::Any + Send>;
  fn safe_call(&self, v: T) -> std::result::Result<R, Self::Error> {
    std::panic::catch_unwind(|| self(v))
  }
}

pub enum SafeWork<T, R> {
  NoTimeout(Arc<dyn Fn(T) -> R + RefUnwindSafe + Send + Sync>),
  WithTimeout(
    Duration,
    Arc<dyn Fn(Option<T>) -> R + RefUnwindSafe + Send + Sync>,
  ),
  WithTimer(
    Duration,
    Arc<dyn Fn(Option<(T, Sender<R>)>) -> bool + Send + Sync + RefUnwindSafe>,
  ),
  Empty,
}
impl<T, R> SafeWork<T, R> {
  pub fn no_timeout<F>(f: F) -> Self
  where
    F: Fn(T) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    SafeWork::NoTimeout(Arc::new(f))
  }

  pub fn with_timeout<F>(timeout: Duration, f: F) -> Self
  where
    F: Fn(Option<T>) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    SafeWork::WithTimeout(timeout, Arc::new(f))
  }

  pub fn with_timer<F>(timeout: Duration, f: F) -> Self
  where
    F: Fn(Option<(T, Sender<R>)>) -> bool + Send + RefUnwindSafe + Sync + 'static,
  {
    SafeWork::WithTimer(timeout, Arc::new(f))
  }

  pub fn empty() -> Self {
    SafeWork::Empty
  }
}
impl<T, R> SafeWork<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn run(&self, rx: Receiver<(T, Sender<R>)>) {
    match self {
      SafeWork::NoTimeout(work) => {
        while let Ok((v, done)) = rx.recv() {
          work
            .as_ref()
            .safe_call(v)
            .map(|r| done.must_send(r))
            .unwrap_or_else(handle_panic);
        }
      }
      SafeWork::WithTimeout(timeout, work) => loop {
        match rx.recv_timeout(*timeout) {
          Ok((v, done)) => {
            work
              .as_ref()
              .safe_call(Some(v))
              .map(|r| done.must_send(r))
              .unwrap_or_else(handle_panic);
          }
          Err(RecvTimeoutError::Timeout) => {
            work
              .as_ref()
              .safe_call(None)
              .map(|_| ())
              .unwrap_or_else(handle_panic);
          }
          Err(RecvTimeoutError::Disconnected) => break,
        };
      },
      SafeWork::WithTimer(duration, work) => {
        let mut timer = duration.as_timer();
        loop {
          let arg = match rx.recv_timeout(timer.get_remain()) {
            Ok(v) => Some(v),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => break,
          };
          work
            .as_ref()
            .safe_call(arg)
            .map(|r| match r {
              true => timer.reset(),
              false => timer.check(),
            })
            .unwrap_or_else(handle_panic);
        }
      }
      SafeWork::Empty => {}
    }
  }
}

fn handle_panic<E>(err: E)
where
  E: Any + Debug,
{
  logger::error(format!("panic in safe work {:?}", err))
}

pub struct SafeWorkThread<T, R> {
  thread: JoinHandle<()>,
  channel: Sender<(T, Sender<R>)>,
}
impl<T, R> SafeWorkThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn new<S: ToString>(name: S, size: usize, work: SafeWork<T, R>) -> Self {
    let work = Arc::new(work);
    let (tx, rx) = unbounded();
    let thread = std::thread::Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn(move || work.as_ref().run(rx))
      .unwrap();
    Self {
      thread,
      channel: tx,
    }
  }

  #[inline]
  pub fn send(&self, v: T) -> Receiver<R> {
    let (done_t, done_r) = unbounded();
    self.channel.must_send((v, done_t));
    done_r
  }

  pub fn send_await(&self, v: T) -> R {
    self.send(v).must_recv()
  }

  pub fn close(self) {
    drop(self.channel);
    if let Err(err) = self.thread.join() {
      logger::error(format!("{:?}", err));
    };
  }
}

pub struct SharedWorkThread<T, R> {
  threads: ArrayQueue<JoinHandle<()>>,
  channel: Sender<(T, Sender<R>)>,
}
impl<T, R> SharedWorkThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn build<S: ToString, F, E>(
    name: S,
    size: usize,
    count: usize,
    build: F,
  ) -> std::result::Result<Self, E>
  where
    F: Fn(usize) -> std::result::Result<SafeWork<T, R>, E>,
  {
    let (tx, rx) = unbounded();
    let threads = ArrayQueue::new(count);
    for i in 0..count {
      let work = Arc::new(build(i)?);
      let rx = rx.clone();
      let th = std::thread::Builder::new()
        .name(format!("{} {}", name.to_string(), i))
        .stack_size(size)
        .spawn(move || work.as_ref().run(rx))
        .unwrap();
      let _ = threads.push(th);
    }
    Ok(Self {
      threads,
      channel: tx,
    })
  }

  #[inline]
  pub fn send(&self, v: T) -> Receiver<R> {
    let (done_t, done_r) = unbounded();
    self.channel.must_send((v, done_t));
    done_r
  }
  pub fn send_await(&self, v: T) -> R {
    self.send(v).must_recv()
  }

  pub fn close(self) {
    drop(self.channel);
    for th in self.threads {
      if let Err(err) = th.join() {
        logger::error(format!("{:?}", err));
      };
    }
  }
}

impl<T, R> RefUnwindSafe for SafeWorkThread<T, R> {}
impl<T, R> RefUnwindSafe for SharedWorkThread<T, R> {}
