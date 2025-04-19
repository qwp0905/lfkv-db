use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  time::Duration,
};

use crossbeam::channel::Sender;

use crate::Result;

use super::{SafeWork, SharedWorkThread, SingleWorkThread};

pub struct WorkBuilder {
  name: String,
  stack_size: usize,
}
impl WorkBuilder {
  pub fn new() -> Self {
    WorkBuilder {
      name: Default::default(),
      stack_size: 2 << 20,
    }
  }
  pub fn name<S: ToString>(mut self, name: S) -> Self {
    self.name = name.to_string();
    self
  }
  pub fn stack_size(mut self, size: usize) -> Self {
    self.stack_size = size;
    self
  }
  pub fn shared(self, count: usize) -> SharedWorkBuilder {
    SharedWorkBuilder {
      builder: self,
      count,
    }
  }
  pub fn single(self) -> SafeWorkBuilder {
    SafeWorkBuilder { builder: self }
  }
}

pub struct SharedWorkBuilder {
  builder: WorkBuilder,
  count: usize,
}
impl SharedWorkBuilder {
  pub fn build<T, R, F, E>(
    self,
    build: F,
  ) -> std::result::Result<SharedWorkThread<T, R>, E>
  where
    T: Send + UnwindSafe + 'static,
    R: Send + 'static,
    F: Fn(usize) -> std::result::Result<SafeWork<T, R>, E>,
  {
    SharedWorkThread::build(
      self.builder.name,
      self.builder.stack_size,
      self.count,
      build,
    )
  }
}
pub struct SafeWorkBuilder {
  builder: WorkBuilder,
}
impl SafeWorkBuilder {
  pub fn no_timeout<T, R, F>(self, f: F) -> SingleWorkThread<T, R>
  where
    T: Send + UnwindSafe + 'static,
    R: Send + 'static,
    F: Fn(T) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    SingleWorkThread::new(
      self.builder.name,
      self.builder.stack_size,
      SafeWork::no_timeout(f),
    )
  }

  pub fn with_timeout<T, R, F>(self, timeout: Duration, f: F) -> SingleWorkThread<T, R>
  where
    T: Send + UnwindSafe + 'static,
    R: Send + 'static,
    F: Fn(Option<T>) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    SingleWorkThread::new(
      self.builder.name,
      self.builder.stack_size,
      SafeWork::with_timeout(timeout, f),
    )
  }

  pub fn with_timer<T, R, F>(self, timeout: Duration, f: F) -> SingleWorkThread<T, R>
  where
    T: Send + UnwindSafe + 'static,
    R: Send + 'static,
    F: Fn(Option<(T, Sender<Result<R>>)>) -> bool + Send + RefUnwindSafe + Sync + 'static,
  {
    SingleWorkThread::new(
      self.builder.name,
      self.builder.stack_size,
      SafeWork::with_timer(timeout, f),
    )
  }
}
#[cfg(test)]
mod tests {
  use crate::{Error, ToArc};

  use super::*;
  use std::sync::atomic::{AtomicUsize, Ordering};
  use std::sync::Arc;
  use std::thread;
  use std::time::Instant;

  #[test]
  fn test_shared_work_thread_no_timeout() {
    let counter = Arc::new(AtomicUsize::new(0));

    let thread = WorkBuilder::new()
      .name("test-no-timeout")
      .stack_size(8192)
      .shared(2)
      .build(|_| {
        let counter_clone = counter.clone();
        Ok(SafeWork::no_timeout(move |x: usize| {
          counter_clone.fetch_add(x, Ordering::SeqCst);
          x * 2
        }))
      })
      .map_err(|e: ()| e)
      .expect("Failed to create thread.");

    // Send multiple tasks
    let results: Vec<_> = (1..5).map(|i| thread.send_await(i).unwrap()).collect();

    assert_eq!(results, vec![2, 4, 6, 8]);
    assert_eq!(counter.load(Ordering::SeqCst), 10); // 1+2+3+4 = 10

    thread.close();
  }

  #[test]
  fn test_shared_work_thread_with_timeout() {
    let counter = Arc::new(AtomicUsize::new(0));

    let thread = WorkBuilder::new()
      .name("test-timeout")
      .stack_size(8192)
      .shared(1)
      .build(|_| {
        let counter_clone = counter.clone();
        Ok(SafeWork::with_timeout(
          Duration::from_millis(100),
          move |x: Option<usize>| {
            if let Some(val) = x {
              counter_clone.fetch_add(val, Ordering::SeqCst);
              val * 2
            } else {
              // When timeout occurs
              counter_clone.fetch_add(1000, Ordering::SeqCst);
              0
            }
          },
        ))
      })
      .map_err(|e: ()| e)
      .expect("Failed to create thread.");

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
    let thread = WorkBuilder::new()
      .name("test-panic")
      .stack_size(8192)
      .single()
      .no_timeout(|x: i32| {
        if x < 0 {
          panic!("Cannot process negative numbers");
        }
        x * 2
      });

    // let thread = SharedWorkThread::new("test-panic", 8192, 1, work);

    // Normal case
    let result = thread.send_await(10);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 20);

    // Panic-inducing case
    let result = thread.send_await(-5);
    assert!(result.is_err());
    match result {
      Err(Error::Panic(_)) => {} // Expected
      _ => panic!("Expected a panic error"),
    }

    thread.close();
  }

  #[test]
  fn test_with_timer() {
    let counter = AtomicUsize::new(0).to_arc();
    let counter_clone = counter.clone();

    let thread = WorkBuilder::new()
      .name("test-timer")
      .stack_size(8192)
      .single()
      .with_timer(Duration::from_millis(50), move |data| {
        if let Some((val, done)) = data {
          counter_clone.fetch_add(val, Ordering::SeqCst);
          let _ = done.send(Ok(val * 2));
          true // Reset timer
        } else {
          counter_clone.fetch_add(1, Ordering::SeqCst);
          false // Continue timer
        }
      });

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

    let thread = WorkBuilder::new()
      .name("test-finalize")
      .stack_size(8192)
      .single()
      .with_timeout(Duration::from_millis(100), move |x: Option<usize>| {
        if let Some(val) = x {
          counter_clone.fetch_add(val, Ordering::SeqCst);
          val * 2
        } else {
          // On timeout or finalization
          counter_clone.fetch_add(100, Ordering::SeqCst);
          0
        }
      });

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
    let thread_count = 4;
    let thread = WorkBuilder::new()
      .name("test-multi")
      .stack_size(8192)
      .shared(thread_count)
      .build(|_| {
        Ok(SafeWork::no_timeout(|x: usize| {
          thread::sleep(Duration::from_millis(50 * x as u64));
          x * 2
        }))
      })
      .map_err(|e: ()| e)
      .expect("Failed to create thread.");
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
