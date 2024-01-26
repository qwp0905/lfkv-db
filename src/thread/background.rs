use std::{panic::UnwindSafe, sync::Arc};

use crate::{ContextReceiver, StoppableChannel, ThreadPool};

pub struct BackgroundThread<T, R> {
  channel: StoppableChannel<T, R>,
  receiver: ContextReceiver<T, R>,
  pool: Arc<ThreadPool>,
}
impl<T, R> BackgroundThread<T, R> {
  pub fn new<F>(&self, f: F)
  where
    T: Send,
    R: Send,
    F: FnOnce(T) -> R + Clone + Send + UnwindSafe + 'static,
  {
    // let rx = self.receiver.clone();
    // self.pool.schedule(move || {
    //   while let Ok(r) = rx.recv_new() {
    //     f(r);
    //   }
    // });
  }
}
