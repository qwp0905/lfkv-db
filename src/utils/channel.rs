use crossbeam::channel::{Receiver, Sender};

pub trait UnwrappedSender<T> {
  fn must_send(&self, t: T);
  fn maybe_send(&self, t: T);
}
impl<T> UnwrappedSender<T> for Sender<T> {
  fn must_send(&self, t: T) {
    self.send(t).unwrap();
  }
  fn maybe_send(&self, t: T) {
    self.send(t).ok();
  }
}

pub trait EmptySender {
  fn close(&self);
}
impl EmptySender for Sender<()> {
  fn close(&self) {
    self.must_send(());
  }
}

pub trait UnwrappedReceiver<T> {
  fn must_recv(&self) -> T;
}
impl<T> UnwrappedReceiver<T> for Receiver<T> {
  fn must_recv(&self) -> T {
    self.recv().unwrap()
  }
}

pub trait DroppableReceiver {
  fn drop_all(&self);
  fn drop_one(&self);
}
impl<T> DroppableReceiver for Receiver<T> {
  fn drop_all(&self) {
    self.iter().for_each(drop);
  }

  fn drop_one(&self) {
    self.recv().ok();
  }
}

pub trait SendBy
where
  Self: Sized,
{
  fn send_by(self, sender: &Sender<Self>);
}
impl<T> SendBy for T {
  fn send_by(self, sender: &Sender<Self>) {
    sender.send(self).unwrap();
  }
}
