use crossbeam::channel::{unbounded, Receiver, Sender};

#[derive(Debug)]
pub enum ThreadContext<T, R = ()> {
  Term,
  New((T, Sender<R>)),
}
impl<T, R> ThreadContext<T, R> {
  pub fn new(value: T) -> (Self, Receiver<R>) {
    let (tx, rx) = unbounded();
    return (ThreadContext::New((value, tx)), rx);
  }

  pub fn done_with(&self, v: R) {
    if let Self::New((_, tx)) = self {
      tx.send(v).unwrap();
    }
  }
}
impl<T> ThreadContext<T, ()> {
  pub fn done(&self) {
    if let Self::New((_, tx)) = self {
      tx.send(()).unwrap();
    }
  }
}

#[derive(Debug)]
pub struct ThreadChannel<T, R = ()> {
  sender: Sender<ThreadContext<T, R>>,
}
impl<T, R> ThreadChannel<T, R> {
  pub fn new() -> (Self, Receiver<ThreadContext<T, R>>) {
    let (tx, rx) = unbounded();
    return (Self { sender: tx }, rx);
  }

  pub fn terminate(&self) {
    self.sender.send(ThreadContext::Term).unwrap();
  }

  pub fn send(&self, v: T) -> Receiver<R> {
    let (tx, rx) = unbounded();
    self.sender.send(ThreadContext::New((v, tx))).unwrap();
    return rx;
  }
}
impl<T, R> Clone for ThreadChannel<T, R> {
  fn clone(&self) -> Self {
    Self {
      sender: self.sender.clone(),
    }
  }
}

pub struct ContextReceiver<T, R> {
  rx: Receiver<ThreadContext<T, R>>,
}
