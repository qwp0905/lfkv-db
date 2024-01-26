use std::panic::UnwindSafe;

use crate::{ContextReceiver, StoppableChannel};

use super::ThreadWorker;

pub struct Background<T, E, R = ()> {
  worker: ThreadWorker<Result<(), E>>,
  chan: StoppableChannel<T, R>,
  rx: ContextReceiver<T, R>,
}
impl<T, E, R> Background<T, E, R> {
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
