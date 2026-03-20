use std::{
  cell::UnsafeCell,
  mem::transmute,
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Arc,
  thread::{park, Builder, JoinHandle, Thread},
};

use crate::{
  utils::{ToArc, UnsafeBorrow, UnsafeBorrowMut},
  Error, Result,
};

use super::{BackgroundThread, Context, OneshotFulfill, SingleFn};
use crossbeam::{queue::SegQueue, utils::Backoff};

fn make_flush<'a, T, R, A>(
  mut when_buffered: SingleFn<'a, Vec<T>, A>,
  mut make_result: SingleFn<'a, &'a A, R>,
) -> impl FnMut(&mut Vec<(T, OneshotFulfill<Result<R>>)>) -> bool + 'a
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
  A: Send + RefUnwindSafe + Sync + 'static,
{
  move |buffered| {
    if buffered.is_empty() {
      return false;
    }

    let (values, waiting): (Vec<_>, Vec<OneshotFulfill<Result<R>>>) =
      buffered.drain(..).unzip();
    match when_buffered.call(values) {
      Ok(r) => waiting
        .into_iter()
        .for_each(|done| done.fulfill(make_result.call(unsafe { transmute(&r) }))),
      Err(_) => waiting
        .into_iter()
        .for_each(|done| done.fulfill(Err(Error::BufferingFailed))),
    };
    true
  }
}

pub struct EagerBufferingThread<T, R> {
  queue: Arc<SegQueue<Context<T, R>>>,
  waker: Thread,
  handle: UnsafeCell<Option<JoinHandle<()>>>,
}
impl<T, R> EagerBufferingThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn new<A, F, E, S: ToString>(
    name: S,
    size: usize,
    count: usize,
    when_buffered: F,
    result: E,
  ) -> Self
  where
    A: Send + RefUnwindSafe + Sync + 'static,
    F: FnMut(Vec<T>) -> A + RefUnwindSafe + Send + Sync + 'static,
    E: for<'a> Fn(&'a A) -> R + Send + Sync + RefUnwindSafe + 'static,
  {
    let queue = SegQueue::new().to_arc();
    let queue_c = Arc::clone(&queue);

    let handle = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn(move || {
        let backoff = Backoff::new();
        let mut buffered = Vec::with_capacity(count);
        let mut flush = make_flush(SingleFn::new(when_buffered), SingleFn::new(result));

        loop {
          'burst: while !backoff.is_completed() {
            'inner: while buffered.len() < count {
              match queue_c.pop() {
                Some(Context::Work(v, done)) => buffered.push((v, done)),
                None => break 'inner,
                Some(Context::Term) => {
                  flush(&mut buffered);
                  return;
                }
              }
            }

            if flush(&mut buffered) {
              backoff.reset();
              continue 'burst;
            };
            backoff.snooze();
          }

          park();
          backoff.reset();
        }
      })
      .unwrap();
    let waker = handle.thread().clone();

    Self {
      queue,
      waker,
      handle: UnsafeCell::new(Some(handle)),
    }
  }
}
impl<T, R> UnwindSafe for EagerBufferingThread<T, R> {}
impl<T, R> RefUnwindSafe for EagerBufferingThread<T, R> {}
unsafe impl<T, R> Send for EagerBufferingThread<T, R> {}
unsafe impl<T, R> Sync for EagerBufferingThread<T, R> {}

impl<T, R> BackgroundThread<T, R> for EagerBufferingThread<T, R> {
  fn register(&self, ctx: Context<T, R>) -> bool {
    if self.handle.get().borrow_unsafe().is_none() {
      return false;
    }
    self.queue.push(ctx);
    self.waker.unpark();
    true
  }

  fn close(&self) {
    if let Some(th) = self.handle.get().borrow_mut_unsafe().take() {
      self.queue.push(Context::Term);
      self.waker.unpark();
      let _ = th.join();
    }
  }
}

#[cfg(test)]
#[path = "tests/eager.rs"]
mod tests;
