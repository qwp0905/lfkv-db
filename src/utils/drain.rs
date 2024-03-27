use std::mem::take;

pub trait Drain {
  fn drain(&mut self) -> Self;
}

impl<T: Default> Drain for T {
  fn drain(&mut self) -> Self {
    take(self)
  }
}
