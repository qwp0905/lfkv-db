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

use super::{oneshot, Context, Oneshot, SafeWork};

pub struct SingleWorkInput<T, R = ()> {
  sender: Sender<Context<T, R>>,
  receiver: Option<Receiver<Context<T, R>>>,
}
impl<T, R> SingleWorkInput<T, R> {
  pub fn new() -> Self {
    let (tx, rx) = unbounded();
    Self {
      sender: tx,
      receiver: Some(rx),
    }
  }
  pub fn send(&self, v: T) -> Oneshot<Result<R>> {
    let (o, f) = oneshot();
    let ctx = Context::Work((v, f));
    self.sender.send(ctx).unwrap();
    o
  }
}
impl<T, R> Clone for SingleWorkInput<T, R> {
  fn clone(&self) -> Self {
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
  pub fn new<S: ToString>(name: S, size: usize, work: SafeWork<T, R>) -> Self {
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
    work: SafeWork<T, R>,
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

  pub fn send(&self, v: T) -> Oneshot<Result<R>> {
    let (done_r, done_t) = oneshot();
    if let Err(TrySendError::Disconnected(_)) =
      self.channel.try_send(Context::Work((v, done_t)))
    {
      drop(done_r);
      let (done_r, done_t) = oneshot();
      done_t.fulfill(Err(Error::WorkerClosed));
      return done_r;
    }
    done_r
  }

  pub fn send_await(&self, v: T) -> Result<R> {
    self.send(v).wait()
  }
  pub fn send_no_wait(&self, v: T) {
    let _ = self.send(v);
  }

  pub fn close(&self) {
    self.channel.must_send(Context::Term);
    if let Some(v) = self.threads.take() {
      let _ = v.join();
    }
  }
}
impl<T, R> RefUnwindSafe for SingleWorkThread<T, R> {}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::atomic::{AtomicUsize, Ordering};
  use std::sync::Arc;
  use std::thread;
  use std::time::Duration;

  const DEFAULT_STACK_SIZE: usize = 4 << 10;

  #[test]
  fn test_shared_work_thread_with_timeout() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    let work =
      SafeWork::with_timeout(Duration::from_millis(100), move |x: Option<usize>| {
        if x.is_none() {
          counter_clone.store(1, Ordering::Release);
        }
      });

    let thread = SingleWorkThread::new("test-timeout", DEFAULT_STACK_SIZE, work);

    // Send a task
    thread.send_await(5).unwrap();

    // Wait a bit to trigger timeout
    thread::sleep(Duration::from_millis(300));

    // Send another task
    thread.send_await(7).unwrap();

    // Check final counter value
    // timeout should called
    assert_eq!(counter.load(Ordering::Acquire), 1);

    thread.close();
  }

  // #[test]
  // fn test_panic_handling() {
  //   let work = SafeWork::no_timeout(|x: i32| {
  //     if x < 0 {
  //       panic!("Cannot process negative numbers");
  //     }
  //     x * 2
  //   });

  //   let thread = SingleWorkThread::new("test-panic", DEFAULT_STACK_SIZE, work);

  //   // Normal case
  //   let result = thread.send_await(10);
  //   assert!(result.is_ok());
  //   assert_eq!(result.unwrap(), 20);

  //   // Panic-inducing case
  //   let result = thread.send_await(-5);
  //   assert!(result.is_err());
  //   if let Err(Error::Panic(_)) = result {
  //     // Panic was converted to Error::Panic as expected
  //   } else {
  //     panic!("Panic was not converted to Error::Panic");
  //   }

  //   thread.close();
  // }
}
