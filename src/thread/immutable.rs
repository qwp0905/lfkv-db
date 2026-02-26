// use std::{
//   panic::{RefUnwindSafe, UnwindSafe},
//   thread::JoinHandle,
// };

// use crossbeam::{
//   channel::{unbounded, Sender, TrySendError},
//   queue::ArrayQueue,
// };

// use crate::{
//   error::{Error, Result},
//   utils::{logger, ToArc, UnwrappedSender},
// };

// use super::{oneshot, Context, Oneshot, SafeWork};

// pub struct SharedWorkThread<T, R = ()> {
//   threads: ArrayQueue<JoinHandle<()>>,
//   channel: Sender<Context<T, R>>,
// }
// impl<T, R> SharedWorkThread<T, R>
// where
//   T: Send + UnwindSafe + 'static,
//   R: Send + 'static,
// {
//   fn new<S: ToString>(name: S, size: usize, count: usize, work: SafeWork<T, R>) -> Self {
//     let (tx, rx) = unbounded();
//     let threads = ArrayQueue::new(count);
//     let work = work.to_arc();
//     for i in 0..count {
//       let work = work.clone();
//       let rx = rx.clone();
//       let th = std::thread::Builder::new()
//         .name(format!("{} {}", name.to_string(), i))
//         .stack_size(size)
//         .spawn(move || work.as_ref().run(rx))
//         .unwrap();
//       let _ = threads.push(th);
//     }
//     Self {
//       threads,
//       channel: tx,
//     }
//   }

//   pub fn build_unchecked<S: ToString, F>(
//     name: S,
//     size: usize,
//     count: usize,
//     build: F,
//   ) -> Self
//   where
//     F: Fn(usize) -> SafeWork<T, R>,
//   {
//     let (tx, rx) = unbounded();
//     let threads = ArrayQueue::new(count);
//     for i in 0..count {
//       let work = build(i);
//       let rx = rx.clone();
//       let th = std::thread::Builder::new()
//         .name(format!("{} {}", name.to_string(), i))
//         .stack_size(size)
//         .spawn(move || work.run(rx))
//         .unwrap();
//       let _ = threads.push(th);
//     }
//     Self {
//       threads,
//       channel: tx,
//     }
//   }

//   pub fn build<S: ToString, F, E>(
//     name: S,
//     size: usize,
//     count: usize,
//     build: F,
//   ) -> std::result::Result<Self, E>
//   where
//     F: Fn(usize) -> std::result::Result<SafeWork<T, R>, E>,
//   {
//     let (tx, rx) = unbounded();
//     let threads = ArrayQueue::new(count);
//     for i in 0..count {
//       let work = build(i)?;
//       let rx = rx.clone();
//       let th = std::thread::Builder::new()
//         .name(format!("{} {}", name.to_string(), i))
//         .stack_size(size)
//         .spawn(move || work.run(rx))
//         .unwrap();
//       let _ = threads.push(th);
//     }
//     Ok(Self {
//       threads,
//       channel: tx,
//     })
//   }

//   #[inline]
//   pub fn send(&self, v: T) -> Oneshot<Result<R>> {
//     let (done_r, done_t) = oneshot();
//     if let Err(TrySendError::Disconnected(_)) =
//       self.channel.try_send(Context::Work((v, done_t)))
//     {
//       drop(done_r);
//       let (done_r, done_t) = oneshot();
//       done_t.fulfill(Err(Error::WorkerClosed));
//       return done_r;
//     }
//     done_r
//   }
//   pub fn send_await(&self, v: T) -> Result<R> {
//     self.send(v).wait()
//   }
//   pub fn send_no_wait(&self, v: T) {
//     let _ = self.send(v);
//   }

//   #[inline]
//   fn join(&self) {
//     while let Some(th) = self.threads.pop() {
//       if let Err(err) = th.join() {
//         logger::error(format!("{:?}", err));
//       }
//     }
//   }

//   pub fn close(&self) {
//     for _ in 0..self.threads.len() {
//       self.channel.must_send(Context::Term);
//     }
//     self.join();
//   }
// }

// impl<T, R> RefUnwindSafe for SingleWorkThread<T, R> {}
// impl<T, R> RefUnwindSafe for SharedWorkThread<T, R> {}

// pub struct SingleWorkThread<T, R = ()>(SharedWorkThread<T, R>);
// impl<T, R> SingleWorkThread<T, R>
// where
//   T: Send + UnwindSafe + 'static,
//   R: Send + 'static,
// {
//   pub fn new<S: ToString>(name: S, size: usize, work: SafeWork<T, R>) -> Self {
//     Self(SharedWorkThread::new(name, size, 1, work))
//   }

//   #[allow(unused)]
//   pub fn build<S: ToString, F, E>(
//     name: S,
//     size: usize,
//     count: usize,
//     build: F,
//   ) -> std::result::Result<Self, E>
//   where
//     F: Fn(usize) -> std::result::Result<SafeWork<T, R>, E>,
//   {
//     Ok(Self(SharedWorkThread::build(name, size, count, build)?))
//   }

//   pub fn send(&self, v: T) -> Oneshot<Result<R>> {
//     self.0.send(v)
//   }

//   pub fn send_await(&self, v: T) -> Result<R> {
//     self.0.send_await(v)
//   }
//   pub fn send_no_wait(&self, v: T) {
//     let _ = self.send(v);
//   }

//   pub fn close(&self) {
//     self.0.close();
//   }
// }
// #[cfg(test)]
// mod tests {
//   use super::*;
//   use std::sync::atomic::{AtomicUsize, Ordering};
//   use std::sync::{Arc, Mutex};
//   use std::thread;
//   use std::time::Duration;

//   const DEFAULT_STACK_SIZE: usize = 4 << 10;

//   #[test]
//   fn test_shared_work_thread_no_timeout() {
//     let m: Arc<Mutex<usize>> = Default::default();
//     let mc = m.clone();
//     let c = AtomicUsize::new(0);
//     let counter = Arc::new(AtomicUsize::new(0));
//     let counter_clone = counter.clone();

//     let work = SafeWork::no_timeout(move |x: usize| {
//       let cc = c.fetch_add(1, Ordering::Release);
//       let mut m = m.lock().unwrap();
//       *m = m.max(cc + 1);
//       drop(m);
//       thread::sleep(Duration::from_millis(100));
//       counter_clone.fetch_add(x, Ordering::Release);
//       c.fetch_sub(1, Ordering::Release);
//       x * 2
//     });

//     let thread_count = 4;
//     let thread =
//       SharedWorkThread::new("test-no-timeout", DEFAULT_STACK_SIZE, thread_count, work);

//     // Send multiple tasks
//     let receivers: Vec<_> = (1..=thread_count).map(|i| thread.send(i)).collect();
//     let results = receivers
//       .into_iter()
//       .map(|receiver| receiver.wait().unwrap())
//       .collect::<Vec<usize>>();

//     assert_eq!(results, vec![2, 4, 6, 8]);
//     assert_eq!(counter.load(Ordering::Acquire), 10); // 1+2+3+4 = 10
//     assert_eq!(*mc.lock().unwrap(), thread_count);

//     thread.close();
//   }

//   #[test]
//   fn test_shared_work_thread_with_timeout() {
//     let counter = Arc::new(AtomicUsize::new(0));
//     let counter_clone = counter.clone();

//     let work =
//       SafeWork::with_timeout(Duration::from_millis(100), move |x: Option<usize>| {
//         if x.is_none() {
//           counter_clone.store(1, Ordering::Release);
//         }
//       });

//     let thread = SharedWorkThread::new("test-timeout", DEFAULT_STACK_SIZE, 1, work);

//     // Send a task
//     thread.send_await(5).unwrap();

//     // Wait a bit to trigger timeout
//     thread::sleep(Duration::from_millis(300));

//     // Send another task
//     thread.send_await(7).unwrap();

//     // Check final counter value
//     // timeout should called
//     assert_eq!(counter.load(Ordering::Acquire), 1);

//     thread.close();
//   }

//   #[test]
//   fn test_panic_handling() {
//     let work = SafeWork::no_timeout(|x: i32| {
//       if x < 0 {
//         panic!("Cannot process negative numbers");
//       }
//       x * 2
//     });

//     let thread = SharedWorkThread::new("test-panic", DEFAULT_STACK_SIZE, 1, work);

//     // Normal case
//     let result = thread.send_await(10);
//     assert!(result.is_ok());
//     assert_eq!(result.unwrap(), 20);

//     // Panic-inducing case
//     let result = thread.send_await(-5);
//     assert!(result.is_err());
//     if let Err(Error::Panic(_)) = result {
//       // Panic was converted to Error::Panic as expected
//     } else {
//       panic!("Panic was not converted to Error::Panic");
//     }

//     thread.close();
//   }

//   #[test]
//   fn test_with_timer() {
//     let counter = Arc::new(AtomicUsize::new(0));
//     let counter_clone = counter.clone();

//     let work = SafeWork::with_timer(Duration::from_millis(50), move |data| {
//       if let Some((val, done)) = data {
//         counter_clone.fetch_add(val, Ordering::Release);
//         let _ = done.fulfill(Ok(val * 2));
//         true // Reset timer
//       } else {
//         counter_clone.fetch_add(1, Ordering::Release);
//         false // Continue timer
//       }
//     });

//     let thread = SharedWorkThread::new("test-timer", DEFAULT_STACK_SIZE, 1, work);

//     // Send a task
//     let receiver = thread.send(10);
//     let result = receiver.wait().unwrap();
//     assert_eq!(result, 20);

//     // Wait a bit to allow multiple timer triggers
//     thread::sleep(Duration::from_millis(225));

//     // Check final counter value (10 + number of timeouts)
//     let final_count = counter.load(Ordering::Acquire);
//     assert!(final_count > 10); // 10 + number of timeouts (hard to predict exactly)

//     thread.close();
//   }

//   #[test]
//   fn test_multiple_threads() {
//     let max = Arc::new(Mutex::new(0));
//     let count = AtomicUsize::new(0);
//     let max_c = max.clone();
//     let work = SafeWork::no_timeout(move |_| {
//       let c = count.fetch_add(1, Ordering::Release);
//       let mut m = max.lock().unwrap();
//       *m = m.max(c + 1);
//       drop(m);
//       thread::sleep(Duration::from_millis(10));
//       count.fetch_sub(1, Ordering::Release);
//     });

//     let thread_count = 4;
//     let thread =
//       SharedWorkThread::new("test-multi", DEFAULT_STACK_SIZE, thread_count, work);

//     let receivers: Vec<_> = (1..=thread_count).map(|i| thread.send(i)).collect();

//     // Collect all results
//     for receiver in receivers.iter() {
//       let _ = receiver.wait().unwrap();
//     }

//     assert_eq!(*max_c.lock().unwrap(), thread_count);

//     thread.close();
//   }
// }
