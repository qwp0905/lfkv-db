use std::time::Duration;

use crossbeam::channel::{
  unbounded, Receiver, RecvError, RecvTimeoutError, Sender,
};

#[allow(unused)]
#[derive(Debug)]
pub enum ThreadContext<T, R = ()> {
  Term,
  WithDone((T, Sender<R>)),
  New(T),
}
#[allow(unused)]
impl<T, R> ThreadContext<T, R> {
  fn with_done(value: T) -> (Self, Receiver<R>) {
    let (tx, rx) = unbounded();
    return (ThreadContext::WithDone((value, tx)), rx);
  }

  fn new(value: T) -> Self {
    return ThreadContext::New(value);
  }
}

#[allow(unused)]
#[derive(Debug)]
pub struct ThreadChannel<T, R = ()> {
  sender: Sender<ThreadContext<T, R>>,
}
#[allow(unused)]
impl<T, R> ThreadChannel<T, R> {
  pub fn new() -> (Self, ContextReceiver<T, R>) {
    let (tx, rx) = unbounded();
    return (Self { sender: tx }, ContextReceiver::new(rx));
  }

  pub fn terminate(&self) {
    self.sender.send(ThreadContext::Term).unwrap();
  }

  pub fn send_with_done(&self, v: T) -> Receiver<R> {
    let (ctx, rx) = ThreadContext::with_done(v);
    self.sender.send(ctx).unwrap();
    return rx;
  }

  pub fn send(&self, v: T) {
    self.sender.send(ThreadContext::new(v)).unwrap();
  }
}
impl<T, R> Clone for ThreadChannel<T, R> {
  fn clone(&self) -> Self {
    Self {
      sender: self.sender.clone(),
    }
  }
}

#[allow(unused)]
pub struct ContextReceiver<T, R = ()> {
  recv: Receiver<ThreadContext<T, R>>,
}
#[allow(unused)]
impl<T, R> ContextReceiver<T, R> {
  fn new(recv: Receiver<ThreadContext<T, R>>) -> Self {
    Self { recv }
  }

  pub fn take_new(&self) -> Result<T, RecvError> {
    if let Ok(ThreadContext::New(v)) = self.recv() {
      return Ok(v);
    };
    return Err(RecvError);
  }

  #[inline]
  pub fn recv(&self) -> Result<ThreadContext<T, R>, RecvError> {
    self.recv.recv()
  }

  #[inline]
  pub fn recv_timeout(
    &self,
    timeout: Duration,
  ) -> Result<ThreadContext<T, R>, RecvTimeoutError> {
    self.recv.recv_timeout(timeout)
  }

  pub fn maybe_timeout(
    &self,
    timeout: Option<Duration>,
  ) -> Result<ThreadContext<T, R>, RecvTimeoutError> {
    timeout
      .map(|to| self.recv_timeout(to))
      .unwrap_or(self.recv().map_err(|_| RecvTimeoutError::Disconnected))
  }
}
