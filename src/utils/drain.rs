use std::mem::take;

pub trait DrainAll {
  fn drain_all(&mut self) -> Self;
}

impl<T: Default> DrainAll for T {
  fn drain_all(&mut self) -> Self {
    take(self)
  }
}
