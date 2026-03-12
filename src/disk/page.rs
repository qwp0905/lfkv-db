use std::{
  cell::UnsafeCell, marker::PhantomData, panic::RefUnwindSafe, ptr::copy_nonoverlapping,
  slice::from_raw_parts,
};

use crate::{
  error::{Error, Result},
  utils::UnsafeBorrow,
};

pub const PAGE_SIZE: usize = 4 << 10; // 4 kb

#[derive(Debug)]
pub struct Page<const T: usize = PAGE_SIZE>(UnsafeCell<[u8; T]>);

impl<const T: usize> Page<T> {
  #[inline]
  pub fn new() -> Self {
    Self(UnsafeCell::new([0; T]))
  }

  #[inline]
  pub fn as_ptr(&self) -> *const u8 {
    self.0.get() as *const u8
  }

  pub fn copy(&self) -> Self {
    let p = Self::new();
    unsafe { copy_nonoverlapping(self.as_ptr(), p.as_ptr() as *mut u8, T) };
    p
  }

  pub fn scanner(&self) -> PageScanner<'_, T> {
    PageScanner::new(self.as_ptr())
  }

  pub fn writer(&mut self) -> PageWriter<'_, T> {
    PageWriter::new(self.0.get() as *mut u8)
  }
}

impl<const T: usize> AsRef<[u8]> for Page<T> {
  fn as_ref(&self) -> &[u8] {
    self.0.get().borrow_unsafe()
  }
}
impl<const T: usize> AsMut<[u8]> for Page<T> {
  fn as_mut(&mut self) -> &mut [u8] {
    self.0.get_mut()
  }
}
impl<const T: usize> From<[u8; T]> for Page<T> {
  fn from(bytes: [u8; T]) -> Self {
    Self(UnsafeCell::new(bytes))
  }
}

impl<const T: usize> From<Vec<u8>> for Page<T> {
  fn from(value: Vec<u8>) -> Self {
    let page = Self::new();
    let len = value.len().min(T);
    unsafe { copy_nonoverlapping(value.as_ptr(), page.as_ptr() as *mut u8, len) };
    page
  }
}
impl<const T: usize> From<&[u8]> for Page<T> {
  fn from(value: &[u8]) -> Self {
    let page = Self::new();
    let len = value.len().min(T);
    unsafe { copy_nonoverlapping(value.as_ptr(), page.as_ptr() as *mut u8, len) };
    page
  }
}
unsafe impl<const T: usize> Send for Page<T> {}
unsafe impl<const T: usize> Sync for Page<T> {}
impl<const T: usize> RefUnwindSafe for Page<T> {}

pub struct PageScanner<'a, const T: usize = PAGE_SIZE> {
  inner: *const u8,
  offset: usize,
  _marker: PhantomData<&'a Page<T>>,
}
impl<'a, const T: usize> PageScanner<'a, T> {
  fn new(inner: *const u8) -> Self {
    Self {
      inner,
      offset: 0,
      _marker: Default::default(),
    }
  }

  pub fn read(&mut self) -> Result<u8> {
    if self.offset >= T {
      return Err(Error::EOF);
    }
    let v = unsafe { *self.inner.add(self.offset) };
    self.offset += 1;
    Ok(v)
  }

  pub fn read_n(&mut self, n: usize) -> Result<&[u8]> {
    let end = self.offset + n;
    if end > T {
      return Err(Error::EOF);
    }
    let b = unsafe { from_raw_parts(self.inner.add(self.offset), n) };
    self.offset = end;
    Ok(b)
  }

  pub fn read_usize(&mut self) -> Result<usize> {
    if self.offset + 8 > T {
      return Err(Error::EOF);
    }
    let v = unsafe { (self.inner.add(self.offset) as *const [u8; 8]).read() };
    self.offset += 8;
    Ok(usize::from_le_bytes(v))
  }

  pub fn read_u16(&mut self) -> Result<u16> {
    if self.offset + 2 > T {
      return Err(Error::EOF);
    }
    let v = unsafe { (self.inner.add(self.offset) as *const [u8; 2]).read() };
    self.offset += 2;
    Ok(u16::from_le_bytes(v))
  }

  pub fn read_u32(&mut self) -> Result<u32> {
    if self.offset + 4 > T {
      return Err(Error::EOF);
    }
    let v = unsafe { (self.inner.add(self.offset) as *const [u8; 4]).read() };
    self.offset += 4;
    Ok(u32::from_le_bytes(v))
  }

  pub fn is_eof(&self) -> bool {
    T <= self.offset
  }
}

pub struct PageWriter<'a, const T: usize = PAGE_SIZE> {
  inner: *mut u8,
  offset: usize,
  marker: PhantomData<&'a Page<T>>,
}
impl<'a, const T: usize> PageWriter<'a, T> {
  fn new(inner: *mut u8) -> Self {
    Self {
      inner,
      offset: 0,
      marker: Default::default(),
    }
  }

  pub fn write(&mut self, bytes: &[u8]) -> Result<()> {
    let len = bytes.len();
    let end = self.offset + len;
    if end > T {
      return Err(Error::EOF);
    };
    unsafe { copy_nonoverlapping(bytes.as_ptr(), self.inner.add(self.offset), len) };
    self.offset = end;
    Ok(())
  }

  pub fn write_usize(&mut self, value: usize) -> Result<()> {
    self.write(&value.to_le_bytes())
  }
  pub fn write_u32(&mut self, value: u32) -> Result {
    self.write(&value.to_le_bytes())
  }

  pub fn is_eof(&self) -> bool {
    T <= self.offset
  }
}

#[cfg(test)]
#[path = "tests/page.rs"]
mod tests;
