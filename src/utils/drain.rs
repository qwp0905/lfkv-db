use std::{collections::BTreeSet, mem::take};

pub trait Drain<T> {
  fn drain(&mut self) -> Self;
}

impl<T> Drain<T> for BTreeSet<T> {
  fn drain(&mut self) -> Self {
    take(self)
  }
}
