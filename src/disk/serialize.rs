use crate::{error::Error, PAGE_SIZE};

use super::Page;

pub trait Serializable<T = Error, const N: usize = PAGE_SIZE>: Sized {
  fn serialize(&self) -> Result<Page<N>, T>;
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
