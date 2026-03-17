use std::{
  cell::UnsafeCell,
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Arc,
  thread::{park_timeout, Builder, JoinHandle},
  time::Duration,
};

use crossbeam::{
  deque::{Injector, Stealer, Worker},
  utils::Backoff,
};

use crate::{
  error::Result,
  utils::{ToArc, UnsafeBorrowMut},
};

// use super::BatchWorkResult;
use super::{oneshot, Context, SharedFn, WorkResult};

fn pop_or_steal<'a, A: 'a>(
  local: &Worker<A>,
  global: &Injector<A>,
  stealers: impl Iterator<Item = &'a Stealer<A>>,
  len: usize,
) -> Option<A> {
  if let Some(task) = local.pop() {
    return Some(task);
  }
  if let Some(task) = global.steal_batch_and_pop(local).success() {
    return Some(task);
  }

  stealers
    .take(len)
    .map(|s| s.steal())
    .flat_map(|v| v.success())
    .next()
}

const THREAD_PARK_TIMEOUT: Duration = Duration::from_micros(100);

fn return_task<A>(global: &Injector<A>, local: &Worker<A>) {
  while let Some(v) = local.pop() {
    global.push(v);
  }
}

fn worker_loop<T, R>(
  local: Worker<Context<T, R>>,
  global: Arc<Injector<Context<T, R>>>,
  stealers: Arc<Vec<Stealer<Context<T, R>>>>,
  work: SharedFn<T, R>,
) -> impl Fn()
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  let backoff = Backoff::new();
  move || {
    let len = stealers.len();
    let mut cycle = stealers.iter().cycle();
    loop {
      if let Some(task) = pop_or_steal(&local, &global, &mut cycle, len) {
        backoff.reset();
        match task {
          Context::Work(data, out) => out.fulfill(work.call(data)),
          Context::Term => return return_task(&global, &local),
        }
        continue;
      }

      if !backoff.is_completed() {
        backoff.snooze();
        continue;
      }

      park_timeout(THREAD_PARK_TIMEOUT);
    }
  }
}

/**
 * More busy wait for heavy workload then shared work thread.
 * Use if performance during bursts is required.
 */
pub struct StealingWorkThread<T, R = ()> {
  threads: UnsafeCell<Vec<JoinHandle<()>>>,
  global: Arc<Injector<Context<T, R>>>,
}
impl<T, R> StealingWorkThread<T, R>
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
    let (stealers, workers): (Vec<_>, Vec<_>) = (0..count)
      .map(|_| Worker::<Context<T, R>>::new_fifo())
      .map(|w| (w.stealer(), w))
      .unzip();

    let global = Injector::new().to_arc();
    let stealers = stealers.to_arc();

    let mut threads = Vec::with_capacity(count);
    for (i, local) in workers.into_iter().enumerate() {
      let thread = Builder::new()
        .name(format!("{}-{}", name.to_string(), i))
        .stack_size(size)
        .spawn(worker_loop(
          local,
          Arc::clone(&global),
          Arc::clone(&stealers),
          SharedFn::new(build(i)?),
        ))
        .unwrap();

      threads.push(thread);
    }

    Ok(Self {
      global,
      threads: UnsafeCell::new(threads),
    })
  }
  // pub fn new<S: ToString, F>(name: S, size: usize, count: usize, build: F) -> Self
  // where
  //   F: Fn(T) -> R + RefUnwindSafe + Send + Sync + 'static,
  // {
  //   let build = build.to_arc();
  //   Self::build__(name, size, count, |_| Ok(build.clone()) as Result<Arc<F>>).unwrap()
  // }

  pub fn build<S: ToString, F, E, W>(
    name: S,
    size: usize,
    count: usize,
    build: F,
  ) -> std::result::Result<Self, E>
  where
    F: Fn(usize) -> std::result::Result<W, E>,
    W: Fn(T) -> R + RefUnwindSafe + Send + Sync + 'static,
  {
    Self::build__(name, size, count, |i| build(i).map(Arc::new))
  }

  #[inline]
  pub fn send(&self, v: T) -> WorkResult<R> {
    let (oneshot, fulfill) = oneshot();
    self.global.push(Context::Work(v, fulfill));
    WorkResult::from(oneshot)
  }
  pub fn send_await(&self, v: T) -> Result<R> {
    self.send(v).wait()
  }
  // pub fn send_no_wait(&self, v: T) {
  //   let _ = self.send(v);
  // }

  // pub fn send_batch(&self, v: impl Iterator<Item = T>) -> BatchWorkResult<R> {
  //   BatchWorkResult::from(v.map(|i| {
  //     let (oneshot, fulfill) = oneshot();
  //     self.global.push(Context::Work(i, fulfill));
  //     oneshot
  //   }))
  // }

  pub fn close(&self) {
    let threads = self.threads.get().borrow_mut_unsafe();
    for _ in 0..threads.len() {
      self.global.push(Context::Term);
    }
    for th in threads.drain(..) {
      th.thread().unpark();
      let _ = th.join();
    }
  }
}

unsafe impl<T, R> Send for StealingWorkThread<T, R> {}
unsafe impl<T, R> Sync for StealingWorkThread<T, R> {}
impl<T, R> RefUnwindSafe for StealingWorkThread<T, R> {}

#[cfg(test)]
#[path = "tests/stealing.rs"]
mod tests;
