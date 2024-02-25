use std::{thread::JoinHandle, time::Duration};

use crossbeam::channel::{unbounded, Receiver, RecvError, RecvTimeoutError, Sender};

use crate::{AsTimer, Timer, UnwrappedReceiver, UnwrappedSender};

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
  pub fn new() -> (Self, ContextReceiver<T, R>) {
    let (tx, rx) = unbounded();
    (Self(tx), ContextReceiver::new(rx))
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
impl<T, R> Drop for StoppableChannel<T, R> {
  fn drop(&mut self) {
    self.terminate()
  }
}

#[allow(unused)]
pub struct ContextReceiver<T, R = ()>(Receiver<StoppableContext<T, R>>);
#[allow(unused)]
impl<T, R> ContextReceiver<T, R> {
  fn new(recv: Receiver<StoppableContext<T, R>>) -> Self {
    Self(recv)
  }

  pub fn recv_new(&self) -> Result<T, RecvError> {
    if let StoppableContext::New(v) = self.recv()? {
      return Ok(v);
    };
    Err(RecvError)
  }

  pub fn recv_new_or_timeout(&self, timeout: Duration) -> Result<Option<T>, RecvError> {
    match self.0.recv_timeout(timeout) {
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
    match self.0.recv_timeout(timeout) {
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
    self.0.recv()
  }

  pub fn recv_all(&self) -> Result<(T, Option<Sender<R>>), RecvError> {
    match self.0.recv()? {
      StoppableContext::Term => Err(RecvError),
      StoppableContext::WithDone((r, t)) => Ok((r, Some(t))),
      StoppableContext::New(r) => Ok((r, None)),
    }
  }

  pub fn maybe_timeout(
    &self,
    timeout: Option<Duration>,
  ) -> Result<StoppableContext<T, R>, RecvTimeoutError> {
    timeout
      .map(|to| self.0.recv_timeout(to))
      .unwrap_or(self.0.recv().map_err(|_| RecvTimeoutError::Disconnected))
  }
}
impl<T, R> Clone for ContextReceiver<T, R> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
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
    std::thread::Builder::new()
      .name(name.to_string())
      .stack_size(stack_size)
      .spawn(move || {
        while let Ok(v) = self.recv_new() {
          f.call(v);
        }
      })
      .unwrap()
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
    std::thread::Builder::new()
      .name(name.to_string())
      .stack_size(stack_size)
      .spawn(move || {
        while let Ok(v) = self.recv_new_or_timeout(timeout) {
          f.call(v);
        }
      })
      .unwrap()
  }

  pub fn to_done<F>(self, name: &str, stack_size: usize, mut f: F) -> JoinHandle<()>
  where
    F: FnMut(T) -> R + Send + 'static,
  {
    std::thread::Builder::new()
      .name(name.to_string())
      .stack_size(stack_size)
      .spawn(move || {
        while let Ok((v, done)) = self.recv_done() {
          let r = f.call(v);
          done.must_send(r);
        }
      })
      .unwrap()
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
    std::thread::Builder::new()
      .name(name.to_string())
      .stack_size(stack_size)
      .spawn(move || {
        while let Ok(v) = self.recv_done_or_timeout(timeout) {
          f.call(v);
        }
      })
      .unwrap()
  }

  pub fn to_thread<F>(self, name: &str, stack_size: usize, mut f: F) -> JoinHandle<()>
  where
    F: FnMut(ContextReceiver<T, R>) + Send + 'static,
  {
    std::thread::Builder::new()
      .name(name.to_string())
      .stack_size(stack_size)
      .spawn(move || f.call(self))
      .unwrap()
  }

  pub fn to_all<F>(self, name: &str, stack_size: usize, mut f: F) -> JoinHandle<()>
  where
    F: FnMut(T) -> R + Send + 'static,
  {
    std::thread::Builder::new()
      .name(name.to_string())
      .stack_size(stack_size)
      .spawn(move || {
        while let Ok((v, done)) = self.recv_all() {
          let r = f.call(v);
          done.map(|t| t.must_send(r));
        }
      })
      .unwrap()
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
    D: Default + Send + 'static,
  {
    std::thread::Builder::new()
      .name(name.to_string())
      .stack_size(stack_size)
      .spawn(move || {
        let mut timer = timeout.as_timer();
        let mut d = Default::default();
        while let Ok(v) = self.recv_done_or_timeout(timer.get_remain()) {
          match f(&mut d, v) {
            true => timer.reset(),
            false => timer.check(),
          }
        }
      })
      .unwrap()
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
