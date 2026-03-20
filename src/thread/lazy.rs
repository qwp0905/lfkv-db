use std::{
  cell::UnsafeCell,
  panic::{RefUnwindSafe, UnwindSafe},
  thread::{Builder, JoinHandle},
  time::Duration,
};

use crate::{
  utils::{AsTimer, UnsafeBorrowMut, UnwrappedSender},
  Result,
};

use super::{BackgroundThread, Context, OneshotFulfill, SingleFn};
use crossbeam::channel::{unbounded, RecvTimeoutError, Sender, TrySendError};

pub struct LazyBufferingThread<T, R> {
  threads: UnsafeCell<Option<JoinHandle<()>>>,
  channel: Sender<Context<T, R>>,
}
impl<T, R> LazyBufferingThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn new<S: ToString>(
    name: S,
    size: usize,
    max_buffering_count: usize,
    timeout: Duration,
    mut when_buffered: SingleFn<'static, (), bool>,
    mut make_result: SingleFn<'static, (T, bool), R>,
  ) -> Self {
    let (tx, rx) = unbounded();

    let th = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn(move || {
        let mut buffered = Vec::with_capacity(max_buffering_count);
        let mut timer = timeout.as_timer();

        let mut flush = |buffer: &mut Vec<(T, OneshotFulfill<Result<R>>)>| {
          let r = when_buffered.call(()).unwrap_or(false);
          for (v, done) in buffer.drain(..) {
            done.fulfill(make_result.call((v, r)));
          }
        };

        loop {
          match rx.recv_timeout(timer.get_remain()) {
            Ok(Context::Work(v, done)) => {
              buffered.push((v, done));
              if buffered.len() < max_buffering_count {
                timer.check();
                continue;
              }
            }
            Ok(Context::Term) | Err(RecvTimeoutError::Disconnected) => {
              return flush(&mut buffered)
            }
            Err(RecvTimeoutError::Timeout) => {}
          }

          if buffered.is_empty() {
            timer.reset();
            continue;
          }

          flush(&mut buffered);
          timer.reset();
        }
      })
      .unwrap();
    Self {
      threads: UnsafeCell::new(Some(th)),
      channel: tx,
    }
  }
}
unsafe impl<T, R> Send for LazyBufferingThread<T, R> {}
unsafe impl<T, R> Sync for LazyBufferingThread<T, R> {}
impl<T, R> UnwindSafe for LazyBufferingThread<T, R> {}
impl<T, R> RefUnwindSafe for LazyBufferingThread<T, R> {}
impl<T, R> BackgroundThread<T, R> for LazyBufferingThread<T, R> {
  fn register(&self, ctx: Context<T, R>) -> bool {
    match self.channel.try_send(ctx) {
      Err(TrySendError::Disconnected(_)) => false,
      _ => true,
    }
  }
  fn close(&self) {
    if let Some(v) = self.threads.get().borrow_mut_unsafe().take() {
      self.channel.must_send(Context::Term);
      let _ = v.join();
    }
  }
}
