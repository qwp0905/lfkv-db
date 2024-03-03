use std::ops::Add;

use crate::utils::size;

use crate::error::{Error, Result};

use super::Serializable;

pub const PAGE_SIZE: usize = size::kb(4) - 24;

#[derive(Debug, PartialEq, Eq)]
pub struct Page<const T: usize = PAGE_SIZE> {
  bytes: [u8; T],
}

impl<const T: usize> Page<T> {
  pub fn new_empty() -> Self {
    let bytes = [0; T];
    Self { bytes }
  }

  pub fn new() -> Self {
    let mut bytes = [0; T];
    bytes[0] = 1;
    Self { bytes }
  }

  fn range_mut(&mut self, start: usize, end: usize) -> &mut [u8] {
    let end = end.min(self.bytes.len());
    self.bytes[start..end].as_mut()
  }

  pub fn copy(&self) -> Self {
    let mut p = Self::new_empty();
    p.as_mut().copy_from_slice(self.as_ref());
    p
  }

  pub fn scanner(&self) -> PageScanner<'_, T> {
    PageScanner::new(&self.bytes)
  }

  pub fn writer(&mut self) -> PageWriter<'_, T> {
    PageWriter::new(&mut self.bytes)
  }

  pub fn is_empty(&self) -> bool {
    self.bytes[0] == 0
  }

  pub fn set_empty(&mut self) {
    self.bytes[0] = 0
  }
}

impl Serializable for Page {
  fn deserialize(value: &Page) -> std::prelude::v1::Result<Self, Error> {
    Ok(value.copy())
  }

  fn serialize(&self) -> std::prelude::v1::Result<Page, Error> {
    Ok(self.copy())
  }
}

impl<const T: usize> AsRef<[u8]> for Page<T> {
  fn as_ref(&self) -> &[u8] {
    &self.bytes
  }
}
impl<const T: usize> AsMut<[u8]> for Page<T> {
  fn as_mut(&mut self) -> &mut [u8] {
    &mut self.bytes
  }
}
impl<const T: usize> From<[u8; T]> for Page<T> {
  fn from(bytes: [u8; T]) -> Self {
    Self { bytes }
  }
}

impl<const T: usize> From<Vec<u8>> for Page<T> {
  fn from(value: Vec<u8>) -> Self {
    let mut page = Self::new();
    let len = value.len().min(T);
    page.range_mut(0, len).copy_from_slice(&value[0..len]);
    page
  }
}
impl<const T: usize> From<Page<T>> for Vec<u8> {
  fn from(value: Page<T>) -> Self {
    value.bytes.into()
  }
}
impl<const T: usize> From<&[u8]> for Page<T> {
  fn from(value: &[u8]) -> Self {
    let mut page = Page::new_empty();
    page.as_mut().copy_from_slice(value);
    page
  }
}

pub struct PageScanner<'a, const T: usize = PAGE_SIZE> {
  inner: &'a [u8; T],
  offset: usize,
}
impl<'a, const T: usize> PageScanner<'a, T> {
  fn new(inner: &'a [u8; T]) -> Self {
    Self { inner, offset: 1 }
  }

  pub fn read(&mut self) -> Result<u8> {
    if let Some(&i) = self.inner.get(self.offset) {
      self.offset += 1;
      return Ok(i);
    }
    Err(Error::EOF)
  }

  pub fn read_n(&mut self, n: usize) -> Result<&[u8]> {
    if self.offset.add(n).gt(&self.inner.len()) {
      return Err(Error::EOF);
    }
    let end = self.offset.add(n);
    let b = self.inner[self.offset..end].as_ref();
    self.offset = end;
    Ok(b)
  }

  pub fn read_usize(&mut self) -> Result<usize> {
    let mut b = [0; 8];
    b.copy_from_slice(self.read_n(8)?);
    Ok(usize::from_be_bytes(b))
  }

  pub fn is_eof(&self) -> bool {
    self.inner.len() <= self.offset
  }
}

pub struct PageWriter<'a, const T: usize = PAGE_SIZE> {
  inner: &'a mut [u8; T],
  offset: usize,
}
impl<'a, const T: usize> PageWriter<'a, T> {
  fn new(inner: &'a mut [u8; T]) -> Self {
    Self { inner, offset: 1 }
  }

  pub fn write(&mut self, bytes: &[u8]) -> Result<()> {
    let end = bytes.len() + self.offset;
    if end >= T {
      return Err(Error::EOF);
    };
    self.inner[self.offset..end].copy_from_slice(&bytes);
    self.offset = end;
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use crate::{Page, PAGE_SIZE};

  #[test]
  fn _1() {
    let mut page = Page::<PAGE_SIZE>::new();
    let mut wt = page.writer();
    wt.write(&[1, 2, 3, 5, 6]).unwrap();

    assert_eq!(page.bytes[0], 1);
    assert_eq!(page.bytes[1], 1);
    assert_eq!(page.bytes[2], 2);
    assert_eq!(page.bytes[3], 3);
    assert_eq!(page.bytes[4], 5);
    assert_eq!(page.bytes[5], 6);
    assert_eq!(page.bytes[6], 0);
  }
}
