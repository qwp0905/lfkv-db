use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Arc,
  thread::{park_timeout, yield_now, Builder, JoinHandle},
  time::Duration,
};

use crossbeam::{
  atomic::AtomicCell,
  deque::{Injector, Stealer, Worker},
};

use crate::{error::Result, utils::ToArc};

use super::{oneshot, Context, Oneshot, SafeFn};

fn steal<A>(
  local: &Worker<A>,
  global: &Injector<A>,
  stealers: &Vec<Stealer<A>>,
) -> Option<A> {
  if let Some(task) = local.pop() {
    return Some(task);
  }
  if let Some(task) = global.steal_batch_and_pop(local).success() {
    return Some(task);
  }

  for stealer in stealers.iter() {
    if let Some(task) = stealer.steal().success() {
      return Some(task);
    }
  }
  None
}

const THREAD_PARK_TIMEOUT: Duration = Duration::from_micros(100);
const YIELD_LIMIT: usize = 16;

fn return_task<A>(global: &Injector<A>, local: &Worker<A>) {
  while let Some(v) = local.pop() {
    global.push(v);
  }
}

fn worker_loop<T, R>(
  local: Worker<Context<T, R>>,
  global: Arc<Injector<Context<T, R>>>,
  stealers: Arc<Vec<Stealer<Context<T, R>>>>,
  work: SafeFn<T, R>,
) -> impl Fn()
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  move || {
    let mut count = 0;
    loop {
      if let Some(task) = steal(&local, &global, &stealers) {
        count = 0;
        match task {
          Context::Work((data, out)) => out.fulfill(work.call(data)),
          Context::Term => return return_task(&global, &local),
        }
        continue;
      }

      if count >= YIELD_LIMIT {
        park_timeout(THREAD_PARK_TIMEOUT);
      } else {
        count += 1;
        yield_now();
      }
    }
  }
}

pub struct SharedWorkThread<T, R = ()> {
  threads: AtomicCell<Vec<JoinHandle<()>>>,
  global: Arc<Injector<Context<T, R>>>,
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
    let (stealers, workers): (Vec<_>, Vec<_>) = (0..count)
      .map(|_| Worker::<Context<T, R>>::new_fifo())
      .map(|w| (w.stealer(), w))
      .unzip();

    let global = Injector::new().to_arc();
    let stealers = stealers.to_arc();

    let mut threads = Vec::with_capacity(count);
    for (i, local) in workers.into_iter().enumerate() {
      let thread = Builder::new()
        .name(format!("{} {}", name.to_string(), i))
        .stack_size(size)
        .spawn(worker_loop(
          local,
          Arc::clone(&global),
          Arc::clone(&stealers),
          SafeFn(build(i)?),
        ))
        .unwrap();

      threads.push(thread);
    }

    Ok(Self {
      global,
      threads: AtomicCell::new(threads),
    })
  }
  pub fn new<S: ToString, F>(name: S, size: usize, count: usize, build: F) -> Self
  where
    F: Fn(T) -> R + RefUnwindSafe + Send + Sync + 'static,
  {
    let build = build.to_arc();
    Self::build__(name, size, count, |_| Ok(build.clone()) as Result<Arc<F>>).unwrap()
  }

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
  pub fn send(&self, v: T) -> Oneshot<Result<R>> {
    let (oneshot, fulfill) = oneshot();
    self.global.push(Context::Work((v, fulfill)));
    oneshot
  }
  pub fn send_await(&self, v: T) -> Result<R> {
    self.send(v).wait()
  }
  pub fn send_no_wait(&self, v: T) {
    let _ = self.send(v);
  }

  pub fn close(&self) {
    let threads = self.threads.take();
    for _ in 0..threads.len() {
      self.global.push(Context::Term);
    }
    for th in threads {
      th.thread().unpark();
      let _ = th.join();
    }
  }
}

impl<T, R> RefUnwindSafe for SharedWorkThread<T, R> {}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::atomic::{AtomicUsize, Ordering};
  use std::sync::Mutex;
  use std::thread;
  use std::time::Duration;

  const DEFAULT_STACK_SIZE: usize = 4 << 10;

  #[test]
  fn test_shared_work_thread_no_timeout() {
    let m: Arc<Mutex<usize>> = Default::default();
    let mc = m.clone();
    let c = AtomicUsize::new(0);
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    let work = move |x: usize| {
      let cc = c.fetch_add(1, Ordering::Release);
      let mut m = m.lock().unwrap();
      *m = m.max(cc + 1);
      drop(m);
      thread::sleep(Duration::from_millis(100));
      counter_clone.fetch_add(x, Ordering::Release);
      c.fetch_sub(1, Ordering::Release);
      x * 2
    };

    let thread_count = 4;
    let thread =
      SharedWorkThread::new("test-no-timeout", DEFAULT_STACK_SIZE, thread_count, work);

    // Send multiple tasks
    let receivers: Vec<_> = (1..=thread_count).map(|i| thread.send(i)).collect();
    let results = receivers
      .into_iter()
      .map(|receiver| receiver.wait().expect("closed"))
      .collect::<Vec<usize>>();

    assert_eq!(results, vec![2, 4, 6, 8]);
    assert_eq!(counter.load(Ordering::Acquire), 10); // 1+2+3+4 = 10
    assert_eq!(*mc.lock().unwrap(), thread_count);

    thread.close();
  }

  #[test]
  fn test_multiple_threads() {
    let max = Arc::new(Mutex::new(0));
    let count = AtomicUsize::new(0);
    let max_c = max.clone();

    let work = move |_| {
      let c = count.fetch_add(1, Ordering::Release);
      let mut m = max.lock().unwrap();
      *m = m.max(c + 1);
      drop(m);
      thread::sleep(Duration::from_millis(10));
      count.fetch_sub(1, Ordering::Release);
    };

    let thread_count = 4;
    let thread =
      SharedWorkThread::new("test-multi", DEFAULT_STACK_SIZE, thread_count, work);

    let receivers: Vec<_> = (1..=thread_count).map(|i| thread.send(i)).collect();

    // Collect all results
    for receiver in receivers.into_iter() {
      let _ = receiver.wait().unwrap();
    }

    assert_eq!(*max_c.lock().unwrap(), thread_count);

    thread.close();
  }
}
