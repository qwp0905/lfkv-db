use std::{
  any::Any,
  fmt::Debug,
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Arc,
  thread::JoinHandle,
  time::Duration,
};

use crossbeam::{
  channel::{unbounded, Receiver, RecvTimeoutError, Sender, TrySendError},
  queue::ArrayQueue,
};

use crate::{
  logger, AsTimer, Error, Result, SafeCallable, SendBy, ToArc, UnwrappedReceiver,
  UnwrappedSender,
};

pub enum Context<T, R> {
  Work((T, Sender<Result<R>>)),
  Finalize,
  Term,
}

pub enum SafeWork<T, R> {
  NoTimeout(Arc<dyn Fn(T) -> R + RefUnwindSafe + Send + Sync>),
  WithTimeout(
    Duration,
    Arc<dyn Fn(Option<T>) -> R + RefUnwindSafe + Send + Sync>,
  ),
  WithTimer(
    Duration,
    Arc<dyn Fn(Option<(T, Sender<Result<R>>)>) -> bool + Send + Sync + RefUnwindSafe>,
  ),
  Empty,
}
impl<T, R> SafeWork<T, R> {
  pub fn no_timeout<F>(f: F) -> Self
  where
    F: Fn(T) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    SafeWork::NoTimeout(f.to_arc())
  }

  pub fn with_timeout<F>(timeout: Duration, f: F) -> Self
  where
    F: Fn(Option<T>) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    SafeWork::WithTimeout(timeout, f.to_arc())
  }

  pub fn with_timer<F>(timeout: Duration, f: F) -> Self
  where
    F: Fn(Option<(T, Sender<Result<R>>)>) -> bool + Send + RefUnwindSafe + Sync + 'static,
  {
    SafeWork::WithTimer(timeout, f.to_arc())
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
  pub fn run(&self, rx: Receiver<Context<T, R>>) {
    match self {
      SafeWork::NoTimeout(work) => {
        while let Ok(Context::Work((v, done))) = rx.recv() {
          work
            .as_ref()
            .safe_call(v)
            .map_err(Error::Panic)
            .send_by(&done);
        }
      }
      SafeWork::WithTimeout(timeout, work) => loop {
        match rx.recv_timeout(*timeout) {
          Ok(Context::Work((v, done))) => {
            work
              .as_ref()
              .safe_call(Some(v))
              .map_err(Error::Panic)
              .send_by(&done);
          }
          Ok(Context::Finalize) => {
            work
              .as_ref()
              .safe_call(None)
              .map(|_| ())
              .unwrap_or_else(handle_panic);
            return;
          }
          Err(RecvTimeoutError::Timeout) => {
            work
              .as_ref()
              .safe_call(None)
              .map(|_| ())
              .unwrap_or_else(handle_panic);
          }
          Ok(Context::Term) | Err(RecvTimeoutError::Disconnected) => return,
        };
      },
      SafeWork::WithTimer(duration, work) => {
        let mut timer = duration.as_timer();
        loop {
          match rx.recv_timeout(timer.get_remain()) {
            Ok(Context::Work((v, done))) => {
              match work.as_ref().safe_call(Some((v, done.clone()))) {
                Ok(true) => timer.reset(),
                Ok(false) => timer.check(),
                Err(err) => done.must_send(Err(Error::Panic(err))),
              };
            }
            Ok(Context::Finalize) => {
              work
                .as_ref()
                .safe_call(None)
                .map(|_| ())
                .unwrap_or_else(handle_panic);
              return;
            }
            Err(RecvTimeoutError::Timeout) => {
              match work.as_ref().safe_call(None) {
                Ok(true) => timer.reset(),
                Ok(false) => timer.check(),
                Err(err) => handle_panic(err),
              };
            }
            Ok(Context::Term) | Err(RecvTimeoutError::Disconnected) => return,
          };
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

pub struct SharedWorkThread<T, R = ()> {
  threads: ArrayQueue<JoinHandle<()>>,
  channel: Sender<Context<T, R>>,
}
impl<T, R> SharedWorkThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  fn new<S: ToString>(name: S, size: usize, count: usize, work: SafeWork<T, R>) -> Self {
    let (tx, rx) = unbounded();
    let threads = ArrayQueue::new(count);
    let work = work.to_arc();
    for i in 0..count {
      let work = work.clone();
      let rx = rx.clone();
      let th = std::thread::Builder::new()
        .name(format!("{} {}", name.to_string(), i))
        .stack_size(size)
        .spawn(move || work.as_ref().run(rx))
        .unwrap();
      let _ = threads.push(th);
    }
    Self {
      threads,
      channel: tx,
    }
  }

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
      let work = build(i)?.to_arc();
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
  pub fn send(&self, v: T) -> Receiver<Result<R>> {
    let (done_t, done_r) = unbounded();
    if let Err(TrySendError::Disconnected(_)) =
      self.channel.try_send(Context::Work((v, done_t)))
    {
      drop(done_r);
      let (done_t, done_r) = unbounded();
      done_t.must_send(Err(Error::WorkerClosed));
      return done_r;
    }
    done_r
  }
  pub fn send_await(&self, v: T) -> Result<R> {
    self.send(v).must_recv()
  }

  #[inline]
  fn join(&self) {
    while let Some(th) = self.threads.pop() {
      if let Err(err) = th.join() {
        logger::error(format!("{:?}", err));
      }
    }
  }

  pub fn close(&self) {
    for _ in 0..self.threads.len() {
      self.channel.must_send(Context::Term);
    }
    self.join();
  }

  pub fn finalize(&self) {
    for _ in 0..self.threads.len() {
      self.channel.must_send(Context::Finalize);
    }
    self.join();
  }
}

impl<T, R> RefUnwindSafe for SingleWorkThread<T, R> {}
impl<T, R> RefUnwindSafe for SharedWorkThread<T, R> {}

pub struct SingleWorkThread<T, R = ()>(SharedWorkThread<T, R>);
impl<T, R> SingleWorkThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn new<S: ToString>(name: S, size: usize, work: SafeWork<T, R>) -> Self {
    Self(SharedWorkThread::new(name, size, 1, work))
  }

  pub fn build<S: ToString, F, E>(
    name: S,
    size: usize,
    count: usize,
    build: F,
  ) -> std::result::Result<Self, E>
  where
    F: Fn(usize) -> std::result::Result<SafeWork<T, R>, E>,
  {
    Ok(Self(SharedWorkThread::build(name, size, count, build)?))
  }

  pub fn send(&self, v: T) -> Receiver<Result<R>> {
    self.0.send(v)
  }

  pub fn send_await(&self, v: T) -> Result<R> {
    self.0.send_await(v)
  }

  pub fn close(&self) {
    self.0.close();
  }

  pub fn finalize(&self) {
    self.0.finalize();
  }
}
#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::atomic::{AtomicUsize, Ordering};
  use std::thread;
  use std::time::Instant;

  #[test]
  fn test_shared_work_thread_no_timeout() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    let work = SafeWork::no_timeout(move |x: usize| {
      thread::sleep(Duration::from_millis(100));
      counter_clone.fetch_add(x, Ordering::SeqCst);
      x * 2
    });

    let thread = SharedWorkThread::new("test-no-timeout", 8192, 4, work);

    let start = Instant::now();
    // Send multiple tasks
    let receivers: Vec<_> = (1..5).map(|i| thread.send(i)).collect();
    let results = receivers
      .into_iter()
      .map(|receiver| receiver.recv().unwrap().unwrap())
      .collect::<Vec<usize>>();
    let elapsed = start.elapsed();

    assert_eq!(results, vec![2, 4, 6, 8]);
    assert_eq!(counter.load(Ordering::SeqCst), 10); // 1+2+3+4 = 10
    assert!(elapsed.as_millis() < 110); // Should take at least 150ms

    thread.close();
  }

  #[test]
  fn test_shared_work_thread_with_timeout() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    let work =
      SafeWork::with_timeout(Duration::from_millis(100), move |x: Option<usize>| {
        if let Some(val) = x {
          counter_clone.fetch_add(val, Ordering::SeqCst);
          val * 2
        } else {
          // When timeout occurs
          counter_clone.fetch_add(1000, Ordering::SeqCst);
          0
        }
      });

    let thread = SharedWorkThread::new("test-timeout", 8192, 1, work);

    // Send a task
    let result = thread.send_await(5).unwrap();
    assert_eq!(result, 10);

    // Wait a bit to trigger timeout
    thread::sleep(Duration::from_millis(130));

    // Send another task
    let result = thread.send_await(7).unwrap();
    assert_eq!(result, 14);

    // Check final counter value
    // 5 + 7 + 1000 (timeout) = 1012
    assert_eq!(counter.load(Ordering::SeqCst), 1012);

    thread.close();
  }

  #[test]
  fn test_panic_handling() {
    let work = SafeWork::no_timeout(|x: i32| {
      if x < 0 {
        panic!("Cannot process negative numbers");
      }
      x * 2
    });

    let thread = SharedWorkThread::new("test-panic", 8192, 1, work);

    // Normal case
    let result = thread.send_await(10);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 20);

    // Panic-inducing case
    let result = thread.send_await(-5);
    assert!(result.is_err());
    if let Err(Error::Panic(_)) = result {
      // Panic was converted to Error::Panic as expected
    } else {
      panic!("Panic was not converted to Error::Panic");
    }

    thread.close();
  }

  #[test]
  fn test_with_timer() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    let work = SafeWork::with_timer(Duration::from_millis(50), move |data| {
      if let Some((val, done)) = data {
        counter_clone.fetch_add(val, Ordering::SeqCst);
        let _ = done.send(Ok(val * 2));
        true // Reset timer
      } else {
        counter_clone.fetch_add(1, Ordering::SeqCst);
        false // Continue timer
      }
    });

    let thread = SharedWorkThread::new("test-timer", 8192, 1, work);

    // Send a task
    let receiver = thread.send(10);
    let result = receiver.recv().unwrap().unwrap();
    assert_eq!(result, 20);

    // Wait a bit to allow multiple timer triggers
    thread::sleep(Duration::from_millis(200));

    // Check final counter value (10 + number of timeouts)
    let final_count = counter.load(Ordering::SeqCst);
    assert!(final_count > 10); // 10 + number of timeouts (hard to predict exactly)

    thread.close();
  }

  #[test]
  fn test_finalize() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    let work =
      SafeWork::with_timeout(Duration::from_millis(100), move |x: Option<usize>| {
        if let Some(val) = x {
          counter_clone.fetch_add(val, Ordering::SeqCst);
          val * 2
        } else {
          // On timeout or finalization
          counter_clone.fetch_add(100, Ordering::SeqCst);
          0
        }
      });

    let thread = SharedWorkThread::new("test-finalize", 8192, 1, work);

    // Send a task
    let result = thread.send_await(5).unwrap();
    assert_eq!(result, 10);

    let count_before_finalize = counter.load(Ordering::SeqCst);

    // Call finalize
    thread.finalize();

    // Check that counter has increased after finalization
    assert_eq!(counter.load(Ordering::SeqCst), count_before_finalize + 100);

    // Channel should be closed after finalization
    let result = thread.send(10);
    assert!(result.recv().unwrap().is_err());
  }

  #[test]
  fn test_multiple_threads() {
    let work = SafeWork::no_timeout(|x: usize| {
      thread::sleep(Duration::from_millis(50 * x as u64))
    });

    let thread_count = 4;
    let thread = SharedWorkThread::new("test-multi", 8192, thread_count, work);

    // Start multiple tasks simultaneously
    let start = Instant::now();

    let receivers: Vec<_> = (1..=thread_count).map(|i| thread.send(i)).collect();

    // Collect all results
    for receiver in receivers.iter() {
      let _ = receiver.recv().unwrap().unwrap();
    }

    let elapsed = start.elapsed();

    // If processed in parallel, total time should be just slightly longer than the longest task (4)
    // If sequential, it would take about 1+2+3+4=10 times longer
    assert!(elapsed < Duration::from_millis(250)); // Generous margin

    thread.close();
  }
}
