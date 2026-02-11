use crate::{error::Error, PAGE_SIZE};

use super::Page;

pub trait Serializable<T = Error, const N: usize = PAGE_SIZE>: Sized {
  fn serialize(&self, page: &mut Page<N>) -> Result<(), T>;
  fn deserialize(value: &Page<N>) -> Result<Self, T>;
}
impl<const N: usize> Page<N> {
  pub fn deserialize<T, E>(&self) -> Result<T, E>
  where
    T: Serializable<E, N>,
  {
    T::deserialize(self)
  }
}

pub trait SerializeFrom<const N: usize, E, T: Serializable<E, N>> {
  fn serialize_from(&mut self, target: &T) -> Result<(), E>;
}
impl<const N: usize, E, T: Serializable<E, N>> SerializeFrom<N, E, T> for Page<N> {
  fn serialize_from(&mut self, target: &T) -> Result<(), E> {
    target.serialize(self)
  }
}
