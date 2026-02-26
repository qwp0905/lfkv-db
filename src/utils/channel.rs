use crossbeam::channel::Sender;

pub trait UnwrappedSender<T> {
  fn must_send(&self, t: T);
}
impl<T> UnwrappedSender<T> for Sender<T> {
  fn must_send(&self, t: T) {
    self.send(t).unwrap();
  }
}
