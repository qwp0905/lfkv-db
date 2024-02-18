use crate::{Page, Result};

pub trait Disk<const N: usize> {
  fn read(&self, index: usize) -> Page<N>;
  fn write(&self, index: usize, page: Page<N>) -> Result;
  fn fsync(&self) -> Result;
}

pub trait BatchWriter<const N: usize> {
  fn batch_write(&self, index: usize, page: Page<N>) -> Result;
}
