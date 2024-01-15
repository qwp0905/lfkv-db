use crate::utils::size;
use bytes::Bytes;

use crate::error::{Error, Result};

use super::Serializable;

pub const PAGE_SIZE: usize = size::kb(4);

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

  pub fn range(&self, start: usize, end: usize) -> &[u8] {
    let end = end.min(self.bytes.len());
    &self.bytes[start..end]
  }

  pub fn range_mut(&mut self, start: usize, end: usize) -> &mut [u8] {
    let end = end.min(self.bytes.len());
    &mut self.bytes[start..end]
  }

  pub fn copy(&self) -> Self {
    let mut p = Self::new();
    p.as_mut().copy_from_slice(self.as_ref());
    return p;
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
    return page;
  }
}
impl<const T: usize> From<Page<T>> for Vec<u8> {
  fn from(value: Page<T>) -> Self {
    value.bytes.into()
  }
}
impl<const T: usize> From<Page<T>> for Bytes {
  fn from(value: Page<T>) -> Self {
    let v: Vec<u8> = value.bytes.into();
    Bytes::from(v)
  }
}
impl<const T: usize> From<Bytes> for Page<T> {
  fn from(value: Bytes) -> Self {
    let mut page = Self::new();
    let end = value.len().min(T);
    page.range_mut(0, end).copy_from_slice(&value[..end]);
    return page;
  }
}

pub struct PageScanner<'a, const T: usize> {
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
    return Err(Error::EOF);
  }

  pub fn read_n(&mut self, n: usize) -> Result<&[u8]> {
    if self.offset + n >= self.inner.len() {
      return Err(Error::EOF);
    }
    let end = self.offset + n;
    let b = &self.inner[self.offset..end];
    self.offset = end;
    return Ok(b);
  }

  pub fn read_usize(&mut self) -> Result<usize> {
    let mut b = [0; 8];
    b.copy_from_slice(self.read_n(8)?);
    return Ok(usize::from_be_bytes(b));
  }

  pub fn is_eof(&self) -> bool {
    self.inner.len() <= self.offset
  }
}

pub struct PageWriter<'a, const T: usize> {
  inner: &'a mut [u8; T],
  offset: usize,
}
impl<'a, const T: usize> PageWriter<'a, T> {
  fn new(inner: &'a mut [u8; T]) -> Self {
    Self { inner, offset: 1 }
  }

  pub fn write(&mut self, bytes: &[u8]) -> Result<()> {
    let end = bytes.len() + self.offset;
    if end >= PAGE_SIZE {
      return Err(Error::EOF);
    };
    self.inner[self.offset..end].copy_from_slice(&bytes);
    self.offset = end;
    return Ok(());
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
