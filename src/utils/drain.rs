use std::collections::BTreeSet;

use crate::replace_default;

pub trait Drain<T> {
  fn drain(&mut self) -> Self;
}

impl<T> Drain<T> for BTreeSet<T> {
  fn drain(&mut self) -> Self {
    replace_default(self)
  }
}
