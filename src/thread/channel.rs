use std::time::Duration;

use crossbeam::channel::{
  unbounded, Receiver, RecvError, RecvTimeoutError, Sender,
};

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
    return (StoppableContext::WithDone((value, tx)), rx);
  }

  fn new(value: T) -> Self {
    return StoppableContext::New(value);
  }
}

#[allow(unused)]
#[derive(Debug)]
pub struct StoppableChannel<T, R = ()>(Sender<StoppableContext<T, R>>);
#[allow(unused)]
impl<T, R> StoppableChannel<T, R> {
  pub fn new() -> (Self, ContextReceiver<T, R>) {
    let (tx, rx) = unbounded();
    return (Self(tx), ContextReceiver::new(rx));
  }

  pub fn terminate(&self) {
    self.0.send(StoppableContext::Term).unwrap();
  }

  pub fn send_with_done(&self, v: T) -> Receiver<R> {
    let (ctx, rx) = StoppableContext::with_done(v);
    self.0.send(ctx).unwrap();
    return rx;
  }

  pub fn send(&self, v: T) {
    self.0.send(StoppableContext::new(v)).unwrap();
  }
}
impl<T, R> Clone for StoppableChannel<T, R> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
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
    return Err(RecvError);
  }

  pub fn recv_new_or_timeout(
    &self,
    timeout: Duration,
  ) -> Result<Option<T>, RecvError> {
    match self.0.recv_timeout(timeout) {
      Ok(c) => {
        if let StoppableContext::New(v) = c {
          return Ok(Some(v));
        };
      }
      Err(RecvTimeoutError::Timeout) => return Ok(None),
      _ => {}
    }
    return Err(RecvError);
  }

  pub fn recv_done(&self) -> Result<(T, Sender<R>), RecvError> {
    if let StoppableContext::WithDone(v) = self.recv()? {
      return Ok(v);
    };
    return Err(RecvError);
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
