use std::{panic::UnwindSafe, thread::JoinHandle};

use crate::{ContextReceiver, StoppableChannel};

pub struct Background<T, R = ()> {
  chan: StoppableChannel<T, R>,
  rx: ContextReceiver<T, R>,
  thread: Option<JoinHandle<()>>,
  name: String,
  stack_size: usize,
}
impl<T, R> Background<T, R> {
  pub fn recv_new(&self) {
    let thread = std::thread::Builder::new()
      .name(self.name.to_owned())
      .stack_size(self.stack_size)
      .spawn(move || {})
      .unwrap();
  }
}
impl<T, R> Drop for Background<T, R> {
  fn drop(&mut self) {
    self.chan.terminate()
  }
}
