use std::{
  cell::UnsafeCell,
  mem::transmute,
  panic::{RefUnwindSafe, UnwindSafe},
  thread::{Builder, JoinHandle},
};

use crate::{
  utils::{UnsafeBorrowMut, UnwrappedSender},
  Error, Result,
};

use super::{BackgroundThread, Context, OneshotFulfill, SingleFn};
use crossbeam::channel::{unbounded, Sender, TryRecvError, TrySendError};

fn make_flush<'a, T, R, A>(
  mut when_buffered: SingleFn<'a, Vec<T>, A>,
  mut make_result: SingleFn<'a, &'a A, R>,
) -> impl FnMut(&mut Vec<(T, OneshotFulfill<Result<R>>)>) + 'a
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
  A: Send + RefUnwindSafe + Sync + 'static,
{
  move |buffered| {
    if buffered.is_empty() {
      return;
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
  }
}

pub struct EagerBufferingThread<T, R> {
  channel: Sender<Context<T, R>>,
  threads: UnsafeCell<Option<JoinHandle<()>>>,
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
    let (tx, rx) = unbounded();
    let th = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn(move || {
        let mut buffered = vec![];
        let mut flush = make_flush(SingleFn::new(when_buffered), SingleFn::new(result));
        loop {
          match rx.recv() {
            Ok(Context::Work(v, done)) => buffered.push((v, done)),
            Err(_) | Ok(Context::Term) => return flush(&mut buffered),
          }

          'burst: while buffered.len() < count {
            match rx.try_recv() {
              Ok(Context::Work(v, done)) => buffered.push((v, done)),
              Err(TryRecvError::Empty) => break 'burst,
              Ok(Context::Term) | Err(TryRecvError::Disconnected) => {
                return flush(&mut buffered)
              }
            }
          }

          flush(&mut buffered);
        }
      })
      .unwrap();
    Self {
      channel: tx,
      threads: UnsafeCell::new(Some(th)),
    }
  }
}
impl<T, R> UnwindSafe for EagerBufferingThread<T, R> {}
impl<T, R> RefUnwindSafe for EagerBufferingThread<T, R> {}
unsafe impl<T, R> Send for EagerBufferingThread<T, R> {}
unsafe impl<T, R> Sync for EagerBufferingThread<T, R> {}

impl<T, R> BackgroundThread<T, R> for EagerBufferingThread<T, R> {
  fn register(&self, ctx: Context<T, R>) -> bool {
    match self.channel.try_send(ctx) {
      Err(TrySendError::Disconnected(_)) => false,
      _ => true,
    }
  }

  fn close(&self) {
    if let Some(th) = self.threads.get().borrow_mut_unsafe().take() {
      self.channel.must_send(Context::Term);
      let _ = th.join();
    }
  }
}

#[cfg(test)]
#[path = "tests/eager.rs"]
mod tests;
