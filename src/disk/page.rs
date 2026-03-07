use std::{marker::PhantomData, ptr::copy_nonoverlapping, slice::from_raw_parts};

use crate::error::{Error, Result};

pub const PAGE_SIZE: usize = 4 << 10; // 4 kb

#[derive(Debug)]
pub struct Page<const T: usize = PAGE_SIZE>([u8; T]);

impl<const T: usize> Page<T> {
  #[inline]
  pub fn new() -> Self {
    Self([0; T])
  }

  #[inline]
  pub fn as_ptr(&self) -> *const u8 {
    self.0.as_ptr()
  }

  pub fn copy(&self) -> Self {
    let p = Self::new();
    unsafe { copy_nonoverlapping(self.as_ptr(), p.as_ptr() as *mut u8, T) };
    p
  }

  pub fn scanner(&self) -> PageScanner<'_, T> {
    PageScanner::new(&self.0)
  }

  pub fn writer(&self) -> PageWriter<'_, T> {
    PageWriter::new(&self)
  }
}

impl<const T: usize> AsRef<[u8]> for Page<T> {
  fn as_ref(&self) -> &[u8] {
    &self.0
  }
}
impl<const T: usize> AsMut<[u8]> for Page<T> {
  fn as_mut(&mut self) -> &mut [u8] {
    &mut self.0
  }
}
impl<const T: usize> From<[u8; T]> for Page<T> {
  fn from(bytes: [u8; T]) -> Self {
    Self(bytes)
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
    let page = Page::new();
    let len = value.len().min(T);
    unsafe { copy_nonoverlapping(value.as_ptr(), page.as_ptr() as *mut u8, len) };
    page
  }
}

pub struct PageScanner<'a, const T: usize = PAGE_SIZE> {
  inner: *const u8,
  offset: usize,
  _marker: PhantomData<&'a Page<T>>,
}
impl<'a, const T: usize> PageScanner<'a, T> {
  fn new(inner: &'a [u8; T]) -> Self {
    Self {
      inner: inner.as_ptr(),
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
  fn new(page: &'a Page<T>) -> Self {
    Self {
      inner: page.0.as_ptr() as *mut u8,
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
mod tests {
  use crate::disk::{Page, PAGE_SIZE};

  #[test]
  fn test_writer() {
    let page = Page::<PAGE_SIZE>::new();
    let mut wt = page.writer();
    wt.write(&[1, 2, 3, 5, 6]).unwrap();

    assert_eq!(page.0[0], 1);
    assert_eq!(page.0[1], 2);
    assert_eq!(page.0[2], 3);
    assert_eq!(page.0[3], 5);
    assert_eq!(page.0[4], 6);
    assert_eq!(page.0[5], 0);
    assert_eq!(page.0[6], 0);
  }

  #[test]
  fn test_read_write() {
    let page = Page::<5>::new();
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
    let page = Page::<PAGE_SIZE>::new();
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
    let page = Page::<SMALL_SIZE>::new();
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
    let page = Page::<PAGE_SIZE>::new();

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
    let page = Page::<PAGE_SIZE>::new();

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
    let page = Page::<PAGE_SIZE>::new();

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
    let page = Page::<PAGE_SIZE>::new();
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
    let page = Page::<PAGE_SIZE>::new();
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
  fn test_read_usize() {
    let page = Page::<15>::new();
    let test_value: usize = 42;

    // Write usize value
    let bytes = test_value.to_le_bytes();
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
    let page = Page::<SIZE>::new();
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
