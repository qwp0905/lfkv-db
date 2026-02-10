use std::ops::{Add, AddAssign, Index, IndexMut};

use crate::error::{Error, Result};
use crate::utils::size;

pub const PAGE_SIZE: usize = size::kb(4);

#[derive(Debug, PartialEq, Eq)]
pub struct Page<const T: usize = PAGE_SIZE> {
  bytes: [u8; T],
}

impl<const T: usize> Page<T> {
  pub fn new() -> Self {
    Self { bytes: [0; T] }
  }

  fn range_mut(&mut self, start: usize, end: usize) -> &mut [u8] {
    let end = end.min(self.bytes.len());
    self.bytes.index_mut(start..end)
  }

  pub fn copy(&self) -> Self {
    let mut p = Self::new();
    p.as_mut().copy_from_slice(self.as_ref());
    p
  }

  pub fn scanner(&self) -> PageScanner<'_, T> {
    PageScanner::new(&self.bytes)
  }

  pub fn writer(&mut self) -> PageWriter<'_, T> {
    PageWriter::new(&mut self.bytes)
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
    page.range_mut(0, len).copy_from_slice(&value.index(0..len));
    page
  }
}
impl<const T: usize> From<Page<T>> for Vec<u8> {
  fn from(value: Page<T>) -> Self {
    value.bytes.into()
  }
}
impl<const T: usize> From<&Page<T>> for Vec<u8> {
  fn from(value: &Page<T>) -> Self {
    value.bytes.into()
  }
}
impl<const T: usize> From<&[u8]> for Page<T> {
  fn from(value: &[u8]) -> Self {
    let mut page = Page::new();
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
    Self { inner, offset: 0 }
  }

  pub fn read(&mut self) -> Result<u8> {
    if let Some(&i) = self.inner.get(self.offset) {
      self.offset.add_assign(1);
      return Ok(i);
    }
    Err(Error::EOF)
  }

  pub fn read_n(&mut self, n: usize) -> Result<&[u8]> {
    let end = self.offset.add(n);
    if end.gt(&self.inner.len()) {
      return Err(Error::EOF);
    }

    let b = self.inner.index(self.offset..end);
    self.offset = end;
    Ok(b)
  }

  pub fn read_usize(&mut self) -> Result<usize> {
    let b = self.read_n(8)?.try_into().map_err(|_| Error::EOF)?;
    Ok(usize::from_be_bytes(b))
  }

  pub fn read_u16(&mut self) -> Result<u16> {
    let b = self.read_n(2)?.try_into().map_err(|_| Error::EOF)?;
    Ok(u16::from_be_bytes(b))
  }

  pub fn is_eof(&self) -> bool {
    self.inner.len().le(&self.offset)
  }
}

pub struct PageWriter<'a, const T: usize = PAGE_SIZE> {
  inner: &'a mut [u8; T],
  offset: usize,
}
impl<'a, const T: usize> PageWriter<'a, T> {
  fn new(inner: &'a mut [u8; T]) -> Self {
    Self { inner, offset: 0 }
  }

  pub fn write(&mut self, bytes: &[u8]) -> Result<()> {
    let end = self.offset.add(bytes.len());
    if end.gt(&T) {
      return Err(Error::EOF);
    };
    self
      .inner
      .index_mut(self.offset..end)
      .copy_from_slice(&bytes);
    self.offset = end;
    Ok(())
  }

  pub fn write_usize(&mut self, value: usize) -> Result<()> {
    self.write(&value.to_be_bytes())
  }

  pub fn is_eof(&self) -> bool {
    self.inner.len().le(&self.offset)
  }
}

#[cfg(test)]
mod tests {
  use crate::{Page, PAGE_SIZE};

  #[test]
  fn test_writer() {
    let mut page = Page::<PAGE_SIZE>::new();
    let mut wt = page.writer();
    wt.write(&[1, 2, 3, 5, 6]).unwrap();

    assert_eq!(page.bytes[0], 1);
    assert_eq!(page.bytes[1], 2);
    assert_eq!(page.bytes[2], 3);
    assert_eq!(page.bytes[3], 5);
    assert_eq!(page.bytes[4], 6);
    assert_eq!(page.bytes[5], 0);
    assert_eq!(page.bytes[6], 0);
  }

  #[test]
  fn test_read_write() {
    let mut page = Page::<5>::new();
    let test_data = [1, 2, 3, 4, 5];

    // Write test
    let mut writer = page.writer();
    assert!(!writer.is_eof());
    writer.write(&test_data).unwrap();
    assert!(writer.is_eof());

    // Read test
    let mut scanner = page.scanner();
    for &expected in test_data.iter() {
      assert_eq!(scanner.read().unwrap(), expected);
    }
  }

  #[test]
  fn test_read_n() {
    let mut page = Page::<PAGE_SIZE>::new();
    let test_data = [1, 2, 3, 4, 5];

    // Write test
    let mut writer = page.writer();
    writer.write(&test_data).unwrap();

    // Read test using read_n
    let mut scanner = page.scanner();
    let read_data = scanner.read_n(test_data.len()).unwrap();
    assert_eq!(read_data, &test_data);
  }

  #[test]
  fn test_write_overflow() {
    const SMALL_SIZE: usize = 5;
    let mut page = Page::<SMALL_SIZE>::new();
    let test_data = [1, 2, 3, 4, 5, 6]; // Data larger than SMALL_SIZE

    let mut writer = page.writer();
    assert!(writer.write(&test_data).is_err()); // Expect EOF error
  }

  #[test]
  fn test_read_eof() {
    const SMALL_SIZE: usize = 5;
    let page = Page::<SMALL_SIZE>::new();
    let mut scanner = page.scanner();

    // Read entire data
    for _ in 0..SMALL_SIZE {
      assert!(scanner.read().is_ok());
    }

    assert!(scanner.is_eof());
    // Attempt to read at EOF
    assert!(scanner.read().is_err());
  }

  #[test]
  fn test_read_n_overflow() {
    const SMALL_SIZE: usize = 5;
    let page = Page::<SMALL_SIZE>::new();
    let mut scanner = page.scanner();

    // Request size larger than page size
    assert!(scanner.read_n(SMALL_SIZE + 1).is_err());
  }

  #[test]
  fn test_sequential_operations() {
    let mut page = Page::<PAGE_SIZE>::new();

    // First write operation (starts from offset 0)
    {
      let mut writer = page.writer();
      writer.write(&[1, 2, 3]).unwrap();
    }

    // Second write operation overwrites from the beginning
    {
      let mut writer = page.writer();
      writer.write(&[4, 5]).unwrap();
      writer.write(&[6]).unwrap();
    }

    // Scanner always reads from offset 0
    let mut scanner = page.scanner();
    assert_eq!(scanner.read().unwrap(), 4);
    assert_eq!(scanner.read().unwrap(), 5);
    assert_eq!(scanner.read().unwrap(), 6);
    assert_eq!(scanner.read().unwrap(), 0); // Rest remains as initial value
  }

  #[test]
  fn test_writer_fresh_start() {
    let mut page = Page::<PAGE_SIZE>::new();

    // First write operation
    {
      let mut writer = page.writer();
      writer.write(&[1, 2, 3]).unwrap();
    }

    // New writer only resets offset to 0
    {
      let mut writer = page.writer();
      writer.write(&[7, 8]).unwrap();
    }

    let mut scanner = page.scanner();
    assert_eq!(scanner.read().unwrap(), 7);
    assert_eq!(scanner.read().unwrap(), 8);
    assert_eq!(scanner.read().unwrap(), 3); // Previous data remains in unwritten portion
  }

  #[test]
  fn test_scanner_fresh_start() {
    let mut page = Page::<PAGE_SIZE>::new();

    // Write data
    {
      let mut writer = page.writer();
      writer.write(&[1, 2, 3]).unwrap();
    }

    // First scanner
    {
      let mut scanner = page.scanner();
      assert_eq!(scanner.read().unwrap(), 1);
      assert_eq!(scanner.read().unwrap(), 2);
    }

    // New scanner starts reading from the beginning
    let mut scanner = page.scanner();
    assert_eq!(scanner.read().unwrap(), 1);
    assert_eq!(scanner.read().unwrap(), 2);
    assert_eq!(scanner.read().unwrap(), 3);
  }

  #[test]
  fn test_interleaved_operations() {
    let mut page = Page::<PAGE_SIZE>::new();
    let test_data = [1, 2, 3];

    // First write
    {
      let mut writer = page.writer();
      writer.write(&test_data).unwrap();
    }

    // First read
    {
      let mut scanner = page.scanner();
      for &expected in test_data.iter() {
        assert_eq!(scanner.read().unwrap(), expected);
      }
    }

    // Second write (only resets offset to 0)
    {
      let mut writer = page.writer();
      writer.write(&[4, 5]).unwrap();
    }

    // Final verification (overwritten portion changes, rest remains as previous data)
    let mut scanner = page.scanner();
    assert_eq!(scanner.read().unwrap(), 4);
    assert_eq!(scanner.read().unwrap(), 5);
    assert_eq!(scanner.read().unwrap(), 3); // Third byte remains unchanged
  }

  #[test]
  fn test_page_copy() {
    let mut page = Page::<PAGE_SIZE>::new();
    let test_data = [1, 2, 3, 4, 5];

    // Write data to original page
    let mut writer = page.writer();
    writer.write(&test_data).unwrap();

    // Create copy and verify data
    let copied = page.copy();
    let mut scanner = copied.scanner();
    for &expected in test_data.iter() {
      assert_eq!(scanner.read().unwrap(), expected);
    }

    // Modify original, verify copy remains unchanged
    let mut writer = page.writer();
    writer.write(&[9, 9]).unwrap();

    let mut scanner = copied.scanner();
    for &expected in test_data.iter() {
      assert_eq!(scanner.read().unwrap(), expected);
    }
  }

  #[test]
  fn test_from_array() {
    const SIZE: usize = 5;
    let data = [1, 2, 3, 4, 5];
    let page = Page::<SIZE>::from(data);

    let mut scanner = page.scanner();
    for &expected in data.iter() {
      assert_eq!(scanner.read().unwrap(), expected);
    }
  }

  #[test]
  fn test_from_vec() {
    const SIZE: usize = 5;
    let data = vec![1, 2, 3, 4, 5];
    let page = Page::<SIZE>::from(data.clone());

    let mut scanner = page.scanner();
    for &expected in data.iter() {
      assert_eq!(scanner.read().unwrap(), expected);
    }

    // Test with vec larger than page size
    let large_data = vec![1, 2, 3, 4, 5, 6];
    let page = Page::<SIZE>::from(large_data);

    let mut scanner = page.scanner();
    for i in 0..SIZE {
      assert_eq!(scanner.read().unwrap(), (i + 1) as u8);
    }
  }

  #[test]
  fn test_from_slice() {
    const SIZE: usize = 5;
    let data = [1, 2, 3, 4, 5];
    let page = Page::<SIZE>::from(&data[..]);

    let mut scanner = page.scanner();
    for &expected in data.iter() {
      assert_eq!(scanner.read().unwrap(), expected);
    }
  }

  #[test]
  fn test_into_vec() {
    const SIZE: usize = 5;
    let mut page = Page::<SIZE>::new();
    let test_data = [1, 2, 3, 4, 5];

    let mut writer = page.writer();
    writer.write(&test_data).unwrap();

    // Convert page to Vec<u8>
    let vec: Vec<u8> = page.into();
    assert_eq!(&vec[..SIZE], &test_data);
  }

  #[test]
  fn test_read_usize() {
    let mut page = Page::<15>::new();
    let test_value: usize = 42;

    // Write usize value
    let bytes = test_value.to_be_bytes();
    let mut writer = page.writer();
    writer.write(&bytes).unwrap();

    // Read and verify usize value
    let mut scanner = page.scanner();
    let read_value = scanner.read_usize().unwrap();
    assert_eq!(read_value, test_value);

    // Test EOF handling
    assert!(scanner.read_usize().is_err());
  }

  #[test]
  fn test_as_ref() {
    const SIZE: usize = 5;
    let mut page = Page::<SIZE>::new();
    let test_data = [1, 2, 3, 4, 5];

    let mut writer = page.writer();
    writer.write(&test_data).unwrap();

    // Verify AsRef implementation
    let slice: &[u8] = page.as_ref();
    assert_eq!(slice, &test_data);
  }

  #[test]
  fn test_as_mut() {
    const SIZE: usize = 5;
    let mut page = Page::<SIZE>::new();
    let test_data = [1, 2, 3, 4, 5];

    // Modify through AsMut
    let slice: &mut [u8] = page.as_mut();
    slice.copy_from_slice(&test_data);

    // Verify changes
    let mut scanner = page.scanner();
    for &expected in test_data.iter() {
      assert_eq!(scanner.read().unwrap(), expected);
    }
  }
}
