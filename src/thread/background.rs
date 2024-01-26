use std::panic::UnwindSafe;

use crate::{ContextReceiver, StoppableChannel};

use super::ThreadWorker;

pub struct Background<T, R, E> {
  worker: ThreadWorker<Result<(), E>>,
  chan: StoppableChannel<T, R>,
  rx: ContextReceiver<T, R>,
}
impl<T, R, E> Background<T, R, E> {
  pub fn recv_new<F>(&mut self, f: F)
  where
    T: Send + 'static,
    R: Send + 'static,
    E: 'static,
    F: FnOnce(T) -> Result<R, E> + Clone + Send + UnwindSafe + 'static,
  {
    let rx = self.rx.clone();
    self.worker.execute(move || {
      while let Ok(v) = rx.recv_new() {
        f.clone()(v)?;
      }
      Ok(())
    });
  }
  pub fn recv_done<F>(&mut self, f: F)
  where
    T: Send + 'static,
    R: Send + 'static,
    E: 'static,
    F: FnOnce(T) -> Result<R, E> + Clone + Send + UnwindSafe + 'static,
  {
    let rx = self.rx.clone();
    self.worker.execute(move || {
      while let Ok((v, done)) = rx.recv_done() {
        done.send(f.clone()(v)?).unwrap();
      }
      Ok(())
    });
  }
}
impl<T, R, E> Drop for Background<T, R, E> {
  fn drop(&mut self) {
    self.chan.terminate()
  }
}
