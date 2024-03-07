use std::{
  sync::{Arc, Mutex},
  thread::{Builder, JoinHandle},
  time::Duration,
};

use crossbeam::channel::{unbounded, Receiver, RecvError, RecvTimeoutError, Sender};

use crate::{logger, AsTimer, UnwrappedReceiver, UnwrappedSender};

#[allow(unused)]
#[derive(Debug)]
pub enum StoppableContext<T, R = ()> {
  Term,
  WithDone((T, Sender<R>)),
  New(T),
}
#[allow(unused)]
impl<T, R> StoppableContext<T, R> {
  fn with_done(value: T) -> (Self, Receiver<R>) {
    let (tx, rx) = unbounded();
    (StoppableContext::WithDone((value, tx)), rx)
  }

  fn new(value: T) -> Self {
    StoppableContext::New(value)
  }
}

#[allow(unused)]
#[derive(Debug)]
pub struct StoppableChannel<T, R = ()>(Sender<StoppableContext<T, R>>);
#[allow(unused)]
impl<T, R> StoppableChannel<T, R> {
  pub fn new(manager: ThreadManager) -> (Self, ContextReceiver<T, R>) {
    let (tx, rx) = unbounded();
    (Self(tx), ContextReceiver::new(rx, manager))
  }

  pub fn terminate(&self) {
    self.0.maybe_send(StoppableContext::Term);
  }

  pub fn send_with_done(&self, v: T) -> Receiver<R> {
    let (ctx, rx) = StoppableContext::with_done(v);
    self.0.must_send(ctx);
    rx
  }

  pub fn send_await(&self, v: T) -> R {
    self.send_with_done(v).must_recv()
  }

  pub fn send(&self, v: T) {
    self.0.must_send(StoppableContext::new(v));
  }
}
impl<T, R> Clone for StoppableChannel<T, R> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

#[allow(unused)]
pub struct ContextReceiver<T, R = ()> {
  inner: Receiver<StoppableContext<T, R>>,
  manager: ThreadManager,
}
#[allow(unused)]
impl<T, R> ContextReceiver<T, R> {
  fn new(inner: Receiver<StoppableContext<T, R>>, manager: ThreadManager) -> Self {
    Self { inner, manager }
  }

  pub fn recv_new(&self) -> Result<T, RecvError> {
    if let StoppableContext::New(v) = self.recv()? {
      return Ok(v);
    };
    Err(RecvError)
  }

  pub fn recv_new_or_timeout(&self, timeout: Duration) -> Result<Option<T>, RecvError> {
    match self.inner.recv_timeout(timeout) {
      Ok(c) => {
        if let StoppableContext::New(v) = c {
          return Ok(Some(v));
        };
      }
      Err(RecvTimeoutError::Timeout) => return Ok(None),
      _ => {}
    }
    Err(RecvError)
  }

  pub fn recv_done_or_timeout(
    &self,
    timeout: Duration,
  ) -> Result<Option<(T, Sender<R>)>, RecvError> {
    match self.inner.recv_timeout(timeout) {
      Ok(c) => {
        if let StoppableContext::WithDone(v) = c {
          return Ok(Some(v));
        };
      }
      Err(RecvTimeoutError::Timeout) => return Ok(None),
      _ => {}
    }
    Err(RecvError)
  }

  pub fn recv_done(&self) -> Result<(T, Sender<R>), RecvError> {
    if let StoppableContext::WithDone(v) = self.recv()? {
      return Ok(v);
    };
    Err(RecvError)
  }

  pub fn recv(&self) -> Result<StoppableContext<T, R>, RecvError> {
    self.inner.recv()
  }

  pub fn recv_all(&self) -> Result<(T, Option<Sender<R>>), RecvError> {
    match self.inner.recv()? {
      StoppableContext::Term => Err(RecvError),
      StoppableContext::WithDone((r, t)) => Ok((r, Some(t))),
      StoppableContext::New(r) => Ok((r, None)),
    }
  }

  pub fn maybe_timeout(
    &self,
    timeout: Option<Duration>,
  ) -> Result<StoppableContext<T, R>, RecvTimeoutError> {
    timeout.map(|to| self.inner.recv_timeout(to)).unwrap_or(
      self
        .inner
        .recv()
        .map_err(|_| RecvTimeoutError::Disconnected),
    )
  }
}
impl<T, R> Clone for ContextReceiver<T, R> {
  fn clone(&self) -> Self {
    Self::new(self.inner.clone(), self.manager.clone())
  }
}
impl<T, R> ContextReceiver<T, R>
where
  T: Send + 'static,
  R: Send + 'static,
{
  pub fn to_new<F>(self, name: &str, stack_size: usize, mut f: F) -> JoinHandle<()>
  where
    F: FnMut(T) -> R + Send + 'static,
  {
    spawn(name, stack_size, move || {
      while let Ok(v) = self.recv_new() {
        f.call(v);
      }
    })
  }

  pub fn to_new_or_timeout<F>(
    self,
    name: &str,
    stack_size: usize,
    timeout: Duration,
    mut f: F,
  ) -> JoinHandle<()>
  where
    F: FnMut(Option<T>) -> R + Send + 'static,
  {
    spawn(name, stack_size, move || {
      while let Ok(v) = self.recv_new_or_timeout(timeout) {
        f.call(v);
      }
    })
  }

  pub fn to_done<F>(self, name: &str, stack_size: usize, mut f: F) -> JoinHandle<()>
  where
    F: FnMut(T) -> R + Send + 'static,
  {
    spawn(name, stack_size, move || {
      while let Ok((v, done)) = self.recv_done() {
        let r = f.call(v);
        done.must_send(r);
      }
    })
  }

  pub fn to_done_or_timeout<F>(
    self,
    name: &str,
    stack_size: usize,
    timeout: Duration,
    mut f: F,
  ) -> JoinHandle<()>
  where
    F: FnMut(Option<(T, Sender<R>)>) -> R + Send + 'static,
  {
    spawn(name, stack_size, move || {
      while let Ok(v) = self.recv_done_or_timeout(timeout) {
        f.call(v);
      }
    })
  }

  pub fn to_thread<F>(self, name: &str, stack_size: usize, mut f: F) -> JoinHandle<()>
  where
    F: FnMut(ContextReceiver<T, R>) + Send + 'static,
  {
    spawn(name, stack_size, move || f.call(self))
  }

  pub fn to_all<F>(self, name: &str, stack_size: usize, mut f: F) -> JoinHandle<()>
  where
    F: FnMut(T) -> R + Send + 'static,
  {
    spawn(name, stack_size, move || {
      while let Ok((v, done)) = self.recv_all() {
        let r = f.call(v);
        done.map(|t| t.must_send(r));
      }
    })
  }

  pub fn with_timer<F, D>(
    self,
    name: &str,
    stack_size: usize,
    timeout: Duration,
    mut f: F,
  ) -> JoinHandle<()>
  where
    F: FnMut(&mut D, Option<(T, Sender<R>)>) -> bool + Send + 'static,
    D: Default,
  {
    spawn(name, stack_size, move || {
      let mut timer = timeout.as_timer();
      let mut d = Default::default();
      while let Ok(v) = self.recv_done_or_timeout(timer.get_remain()) {
        match f(&mut d, v) {
          true => timer.reset(),
          false => timer.check(),
        }
      }
    })
  }
}

pub trait Callable<T, R> {
  fn call(&mut self, v: T) -> R;
}
impl<T, R, F: FnMut(T) -> R> Callable<T, R> for F {
  fn call(&mut self, v: T) -> R {
    self(v)
  }
}

fn spawn<F, R>(name: &str, size: usize, f: F) -> JoinHandle<R>
where
  R: Send + 'static,
  F: FnOnce() -> R + Send + 'static,
{
  let s = name.to_string();
  Builder::new()
    .name(s.clone())
    .stack_size(size)
    .spawn(move || {
      let r = f();
      logger::info(format!("{} thread done", s));
      r
    })
    .unwrap()
}

// pub struct ThreadManager {
//   back: Option<JoinHandle<()>>,
//   channel: StoppableChannel<Option<JoinHandle<()>>>,
// }
// impl ThreadManager {
//   pub fn new() -> Self {
//     let (channel, rx) = StoppableChannel::new();
//     let mut v = vec![];
//     let back = rx.to_all("threads", 10, move |t| match t {
//       Some(t) => v.push(t),
//       None => v.drain(..).for_each(|th: JoinHandle<()>| {
//         if th.is_finished() {
//           return;
//         }
//         if let Err(err) = th.join() {
//           logger::error(format!("{:?}", err))
//         }
//       }),
//     });

//     Self {
//       back: Some(back),
//       channel,
//     }
//   }
// }
// impl Drop for ThreadManager {
//   fn drop(&mut self) {
//     self.channel.send_await(None);
//     if let Some(t) = self.back.take() {
//       if t.is_finished() {
//         return;
//       }

//       self.channel.terminate();
//       if let Err(err) = t.join() {
//         logger::error(format!("{:?}", err))
//       }
//     }
//   }
// }

pub struct ThreadManager {
  threads: Arc<Mutex<Vec<JoinHandle<()>>>>,
}
impl ThreadManager {
  pub fn generate<T, R>(&self) -> (StoppableChannel<T, R>, ContextReceiver<T, R>) {
    StoppableChannel::new(self.clone())
  }

  pub fn push(&self) {}
}
impl Clone for ThreadManager {
  fn clone(&self) -> Self {
    Self {
      threads: self.threads,
    }
  }
}
