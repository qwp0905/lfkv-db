use crossbeam::channel::{Receiver, Sender};

pub trait EmptySender {
  fn close(&self);
}
impl EmptySender for Sender<()> {
  fn close(&self) {
    self.send(()).unwrap();
  }
}

pub trait DroppableReceiver {
  fn drop_all(&self) {}
}
impl<T> DroppableReceiver for Receiver<T> {
  fn drop_all(&self) {
    self.iter().for_each(drop);
  }
}
