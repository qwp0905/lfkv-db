use crate::utils::size;
use bytes::Bytes;

use crate::error::{Error, Result};

pub const PAGE_SIZE: usize = size::kb(4);

#[derive(Debug, PartialEq, Eq)]
pub struct Page {
  bytes: [u8; PAGE_SIZE],
}
impl Page {
  pub fn new() -> Self {
    let mut bytes = [0; PAGE_SIZE];
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

  pub fn scanner(&self) -> PageScanner<'_> {
    PageScanner::new(&self.bytes)
  }

  pub fn writer(&mut self) -> PageWriter<'_> {
    PageWriter::new(&mut self.bytes)
  }

  pub fn is_empty(&self) -> bool {
    self.bytes[0] == 0
  }

  pub fn set_empty(&mut self) {
    self.bytes[0] = 0
  }
}

impl AsRef<[u8]> for Page {
  fn as_ref(&self) -> &[u8] {
    &self.bytes
  }
}
impl AsMut<[u8]> for Page {
  fn as_mut(&mut self) -> &mut [u8] {
    &mut self.bytes
  }
}
impl From<[u8; PAGE_SIZE]> for Page {
  fn from(bytes: [u8; PAGE_SIZE]) -> Self {
    Self { bytes }
  }
}

impl From<Vec<u8>> for Page {
  fn from(value: Vec<u8>) -> Self {
    let mut page = Self::new();
    let len = value.len().min(PAGE_SIZE);
    page.range_mut(0, len).copy_from_slice(&value[0..len]);
    return page;
  }
}
impl From<Page> for Vec<u8> {
  fn from(value: Page) -> Self {
    value.bytes.into()
  }
}
impl From<Page> for Bytes {
  fn from(value: Page) -> Self {
    let v: Vec<u8> = value.bytes.into();
    Bytes::from(v)
  }
}
impl From<Bytes> for Page {
  fn from(value: Bytes) -> Self {
    let mut page = Self::new();
    let end = value.len().min(PAGE_SIZE);
    page.range_mut(0, end).copy_from_slice(&value[..end]);
    return page;
  }
}

pub struct PageScanner<'a> {
  inner: &'a [u8; PAGE_SIZE],
  offset: usize,
}
impl<'a> PageScanner<'a> {
  fn new(inner: &'a [u8; PAGE_SIZE]) -> Self {
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

pub struct PageWriter<'a> {
  inner: &'a mut [u8; PAGE_SIZE],
  offset: usize,
}
impl<'a> PageWriter<'a> {
  fn new(inner: &'a mut [u8; PAGE_SIZE]) -> Self {
    Self { inner, offset: 1 }
  }

  pub fn write(&mut self, bytes: &[u8]) -> Result<()> {
    let end = bytes.len() + self.offset;
    if end >= PAGE_SIZE {
      return Err(Error::EOF);
    };
    self.inner[self.offset..end].copy_from_slice(&bytes);
    return Ok(());
  }
}
