use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

const DEFAULT_STACK_SIZE: usize = 64 << 10;

#[test]
fn test_no_timeout() {
  let counter = Arc::new(AtomicUsize::new(0));
  let counter_c = counter.clone();

  let thread_count = 4;
  let thread = StealingWorkThread::new(
    "test-no-timeout",
    DEFAULT_STACK_SIZE,
    thread_count,
    move |x: usize| {
      thread::sleep(Duration::from_millis(100));
      counter_c.fetch_add(x, Ordering::Release);
      x * 2
    },
  );

  // Send multiple tasks
  let receivers: Vec<_> = (1..=thread_count).map(|i| thread.send(i)).collect();
  let results = receivers
    .into_iter()
    .map(|receiver| receiver.wait().expect("closed"))
    .collect::<Vec<usize>>();

  assert_eq!(results, vec![2, 4, 6, 8]);
  assert_eq!(counter.load(Ordering::Acquire), 10); // 1+2+3+4 = 10

  thread.close();
}

#[test]
fn test_multiple_threads() {
  let max = Arc::new(Mutex::new(0));
  let max_c = max.clone();
  let count = Arc::new(AtomicUsize::new(0));

  let thread_count = 4;
  let thread =
    StealingWorkThread::new("test-multi", DEFAULT_STACK_SIZE, thread_count, move |_| {
      let c = count.fetch_add(1, Ordering::Release);
      let mut m = max_c.lock().unwrap();
      *m = m.max(c + 1);
      drop(m);
      thread::sleep(Duration::from_millis(10));
      count.fetch_sub(1, Ordering::Release);
    });

  let receivers: Vec<_> = (0..(thread_count << 1)).map(|i| thread.send(i)).collect();

  // Collect all results
  for receiver in receivers.into_iter() {
    let _ = receiver.wait().unwrap();
  }

  assert_eq!(*max.lock().unwrap(), thread_count);

  thread.close();
}

#[test]
fn test_multiple_close() {
  let thread_count = 4;
  let thread = StealingWorkThread::new(
    "test-multi-close",
    DEFAULT_STACK_SIZE,
    thread_count,
    |_: ()| (),
  );

  thread.close();
  thread.close();
  thread.close();
}
