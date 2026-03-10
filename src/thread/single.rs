use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  thread::{Builder, JoinHandle},
};

use crossbeam::{
  atomic::AtomicCell,
  channel::{unbounded, Receiver, Sender, TrySendError},
};

use crate::{
  error::{Error, Result},
  utils::UnwrappedSender,
};

use super::{oneshot, Context, SafeWork, WorkResult};

pub struct SingleWorkInput<T, R = ()> {
  sender: Sender<Context<T, R>>,
  receiver: Option<Receiver<Context<T, R>>>,
}
impl<T, R> SingleWorkInput<T, R> {
  pub fn new() -> Self {
    let (sender, receiver) = unbounded();
    Self {
      sender,
      receiver: Some(receiver),
    }
  }
  pub fn send(&self, v: T) -> WorkResult<R> {
    let (done_r, done_t) = oneshot();
    if let Err(TrySendError::Disconnected(_)) =
      self.sender.try_send(Context::Work((v, done_t)))
    {
      drop(done_r);
      let (done_r, done_t) = oneshot();
      done_t.fulfill(Err(Error::WorkerClosed));
      return WorkResult::from(done_r);
    }
    WorkResult::from(done_r)
  }
  pub fn copy(&self) -> Self {
    Self {
      sender: self.sender.clone(),
      receiver: None,
    }
  }
}

pub struct SingleWorkThread<T, R = ()> {
  threads: AtomicCell<Option<JoinHandle<()>>>,
  channel: Sender<Context<T, R>>,
}
impl<T, R> SingleWorkThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn new<S: ToString>(name: S, size: usize, mut work: SafeWork<T, R>) -> Self {
    let (channel, rx) = unbounded();
    let th = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn(move || work.run(rx))
      .unwrap();
    Self {
      threads: AtomicCell::new(Some(th)),
      channel,
    }
  }

  pub fn from_channel<S: ToString>(
    name: S,
    size: usize,
    mut work: SafeWork<T, R>,
    mut input: SingleWorkInput<T, R>,
  ) -> Result<Self> {
    let rx = match input.receiver.take() {
      Some(rx) => rx,
      None => return Err(Error::ThreadConflict),
    };

    let th = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn(move || work.run(rx))
      .unwrap();
    Ok(Self {
      threads: AtomicCell::new(Some(th)),
      channel: input.sender.clone(),
    })
  }

  pub fn send(&self, v: T) -> WorkResult<R> {
    let (done_r, done_t) = oneshot();
    if let Err(TrySendError::Disconnected(_)) =
      self.channel.try_send(Context::Work((v, done_t)))
    {
      drop(done_r);
      let (done_r, done_t) = oneshot();
      done_t.fulfill(Err(Error::WorkerClosed));
      return WorkResult::from(done_r);
    }
    WorkResult::from(done_r)
  }

  pub fn send_await(&self, v: T) -> Result<R> {
    self.send(v).wait()
  }

  pub fn close(&self) {
    if let Some(v) = self.threads.take() {
      self.channel.must_send(Context::Term);
      let _ = v.join();
    }
  }
}
impl<T, R> RefUnwindSafe for SingleWorkThread<T, R> {}

#[cfg(test)]
#[path = "tests/single.rs"]
mod tests;
