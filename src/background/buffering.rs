use std::thread::Builder;

use super::{
  oneshot, Close, Dispatch, ExecutableContext, Execute, OneshotFulfill, SingleFn,
  ThreadSlot, UnwindSpawner,
};

use crossbeam::channel::{unbounded, Receiver, Sender};

type Buffered<T, R> = Vec<(T, Option<OneshotFulfill<R>>)>;

/**
 * Flush buffered items by calling the batch handler once.
 *
 * The handler receives all buffered values and returns one result for the
 * entire batch. That result is cloned to every waiter that submitted a `Work`
 * item in the batch; dispatched items have no waiter.
 */
const fn make_flush<'a, T, R>(
  mut when_buffered: SingleFn<'a, Vec<T>, R>,
) -> impl FnMut(&mut Buffered<T, R>) + 'a
where
  T: Send + 'a,
  R: Send + Clone + 'a,
{
  move |buffered| {
    if buffered.is_empty() {
      return;
    }

    let (values, waiting): (Vec<_>, Vec<_>) = buffered.drain(..).unzip();
    let result = when_buffered.call(values);
    waiting
      .into_iter()
      .flatten()
      .for_each(|done| done.fulfill(result.clone()));
  }
}

const fn worker_loop<T, R>(
  recv: Receiver<ExecutableContext<T, R>>,
  count: usize,
  when_buffered: SingleFn<'static, Vec<T>, R>,
) -> impl FnOnce()
where
  T: Send,
  R: Send + Clone,
{
  move || {
    let mut buffered = Vec::with_capacity(count);
    let mut flush = make_flush(when_buffered);
    'outer: while let Ok(ctx) = recv.recv() {
      match ctx {
        ExecutableContext::Work(v, done) => buffered.push((v, Some(done))),
        ExecutableContext::Dispatch(v) => buffered.push((v, None)),
        ExecutableContext::Term => break 'outer,
      }
      debug_assert_eq!(buffered.len(), 1);

      for ctx in recv.try_iter().take(count - 1) {
        match ctx {
          ExecutableContext::Work(v, done) => buffered.push((v, Some(done))),
          ExecutableContext::Dispatch(v) => buffered.push((v, None)),
          ExecutableContext::Term => break 'outer,
        }
      }
      debug_assert!(buffered.len() <= count);
      flush(&mut buffered);
    }

    flush(&mut buffered)
  }
}

/**
 * Single-worker runtime that processes queued work in buffered batches.
 *
 * The worker drains up to `count` queued items, calls the handler once with the
 * collected `Vec<T>`, and completes all waiters with the returned result. While
 * one batch is being processed, producers can continue pushing new work into
 * the queue; after the flush, the worker immediately drains the next batch.
 */
pub struct BufferingThread<T, R> {
  queue: Sender<ExecutableContext<T, R>>,
  slot: ThreadSlot,
}
impl<T, R> BufferingThread<T, R> {
  pub fn new<S: ToString>(
    name: S,
    size: usize,
    count: usize,
    when_buffered: SingleFn<'static, Vec<T>, R>,
  ) -> Self
  where
    T: Send + 'static,
    R: Send + Clone + 'static,
  {
    let (queue, recv) = unbounded();
    let handle = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn_unwind(worker_loop(recv, count, when_buffered));
    Self {
      queue,
      slot: ThreadSlot::new(handle),
    }
  }

  fn register(&self, ctx: ExecutableContext<T, R>) {
    self.queue.send(ctx).unwrap();
  }
}
impl<T: Send, R: Send> Close for BufferingThread<T, R> {
  fn close(&self) {
    if let Some(th) = self.slot.close() {
      self.register(ExecutableContext::Term);
      th.join().unwrap();
    }
  }
}
impl<T: Send, R: Send> Dispatch<T> for BufferingThread<T, R> {
  fn dispatch(&self, value: T) {
    self.register(ExecutableContext::Dispatch(value))
  }
}
impl<T: Send, R: Send> Execute<T, R> for BufferingThread<T, R> {
  fn execute(&self, value: T) -> super::Oneshot<R> {
    let (o, f) = oneshot();
    self.register(ExecutableContext::Work(value, f));
    o
  }
}

#[cfg(test)]
#[path = "tests/buffering.rs"]
mod tests;
