use std::panic::UnwindSafe;

use super::SharedWorkThread;
use crate::{utils::UnwrappedReceiver, Result};

pub trait SendAll<T, R> {
  fn send_all_to(self, thread: &SharedWorkThread<T, R>) -> Result<Vec<R>>;
}
impl<T, R, E> SendAll<T, R> for E
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
  E: IntoIterator<Item = T>,
{
  fn send_all_to(self, thread: &SharedWorkThread<T, R>) -> Result<Vec<R>> {
    let mut receivers = Vec::new();
    let mut result = Vec::new();
    for v in self {
      receivers.push(thread.send(v));
    }
    for r in receivers {
      result.push(r.must_recv()?)
    }
    Ok(result)
  }
}
