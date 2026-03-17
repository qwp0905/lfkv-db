use std::{
  cell::UnsafeCell,
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Arc,
  thread::{Builder, JoinHandle},
};

use crossbeam::channel::{unbounded, Receiver, Sender};

use crate::{
  error::Result,
  utils::{ToArc, UnsafeBorrowMut, UnwrappedSender},
};

use super::{oneshot, BatchWorkResult, Context, SharedFn, WorkResult};

fn worker_loop<T, R>(receiver: Receiver<Context<T, R>>, work: SharedFn<T, R>) -> impl Fn()
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  move || {
    while let Ok(Context::Work(data, done)) = receiver.recv() {
      done.fulfill(work.call(data));
    }
  }
}

/**
 * Subscribe to shared channels to handle task distribution.
 * Use when some performance is required during bursts but idle time is long.
 */
pub struct SharedWorkThread<T, R = ()> {
  threads: UnsafeCell<Vec<JoinHandle<()>>>,
  queue: Sender<Context<T, R>>,
}
impl<T, R> SharedWorkThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  fn build__<S: ToString, F, E, W>(
    name: S,
    size: usize,
    count: usize,
    build: F,
  ) -> std::result::Result<Self, E>
  where
    F: Fn(usize) -> std::result::Result<Arc<W>, E>,
    W: Fn(T) -> R + RefUnwindSafe + Send + Sync + 'static,
  {
    let (tx, rx) = unbounded();
    let mut threads = Vec::with_capacity(count);
    for i in 0..count {
      let thread = Builder::new()
        .name(format!("{}-{}", name.to_string(), i))
        .stack_size(size)
        .spawn(worker_loop(rx.clone(), SharedFn::new(build(i)?)))
        .unwrap();

      threads.push(thread);
    }

    Ok(Self {
      queue: tx,
      threads: UnsafeCell::new(threads),
    })
  }
  pub fn new<S: ToString, F>(name: S, size: usize, count: usize, build: F) -> Self
  where
    F: Fn(T) -> R + RefUnwindSafe + Send + Sync + 'static,
  {
    let build = build.to_arc();
    Self::build__(name, size, count, |_| Ok(build.clone()) as Result<Arc<F>>).unwrap()
  }

  // pub fn build<S: ToString, F, E, W>(
  //   name: S,
  //   size: usize,
  //   count: usize,
  //   build: F,
  // ) -> std::result::Result<Self, E>
  // where
  //   F: Fn(usize) -> std::result::Result<W, E>,
  //   W: Fn(T) -> R + RefUnwindSafe + Send + Sync + 'static,
  // {
  //   Self::build__(name, size, count, |i| build(i).map(Arc::new))
  // }

  #[inline]
  pub fn send(&self, v: T) -> WorkResult<R> {
    let (oneshot, fulfill) = oneshot();
    self.queue.must_send(Context::Work(v, fulfill));
    WorkResult::from(oneshot)
  }
  // pub fn send_await(&self, v: T) -> Result<R> {
  //   self.send(v).wait()
  // }
  pub fn send_no_wait(&self, v: T) {
    let _ = self.send(v);
  }

  pub fn send_batch(&self, v: impl Iterator<Item = T>) -> BatchWorkResult<R> {
    BatchWorkResult::from(v.map(|i| {
      let (oneshot, fulfill) = oneshot();
      self.queue.must_send(Context::Work(i, fulfill));
      oneshot
    }))
  }

  pub fn close(&self) {
    let threads = self.threads.get().borrow_mut_unsafe();
    for _ in 0..threads.len() {
      self.queue.must_send(Context::Term);
    }
    for th in threads.drain(..) {
      let _ = th.join();
    }
  }
}

unsafe impl<T, R> Send for SharedWorkThread<T, R> {}
unsafe impl<T, R> Sync for SharedWorkThread<T, R> {}
impl<T, R> RefUnwindSafe for SharedWorkThread<T, R> {}

#[cfg(test)]
#[path = "tests/shared.rs"]
mod tests;
