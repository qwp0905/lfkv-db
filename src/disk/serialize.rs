use crate::error::Error;

use super::Page;

pub trait Serializable<T = Error>: Sized {
  fn serialize(&self) -> Result<Page, T>;
  fn deserialize(value: &Page) -> Result<Self, T>;
}
impl Page {
  pub fn deserialize<T, E>(&self) -> Result<T, E>
  where
    T: Serializable<E>,
  {
    T::deserialize(self)
  }
}
