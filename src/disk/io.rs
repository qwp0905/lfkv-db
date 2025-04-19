use std::{fs::File, io::Result};

#[cfg(any(target_os = "macos", target_os = "linux"))]
use std::os::unix::fs::FileExt;

#[cfg(target_os = "windows")]
use std::os::windows::fs::FileExt;

pub trait Pread {
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
}
impl Pread for File {
  #[cfg(any(target_os = "macos", target_os = "linux"))]
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    self.read_at(buf, offset)
  }

  #[cfg(target_os = "windows")]
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    self.seek_read(buf, offset)
  }
}
pub trait Pwrite {
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize>;
}
impl Pwrite for File {
  #[cfg(any(target_os = "macos", target_os = "linux"))]
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
    self.write_at(buf, offset)
  }

  #[cfg(target_os = "windows")]
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
    self.seek_write(buf, offset)
  }
}
pub trait Append {
  fn append(&self, buf: &[u8]) -> Result<usize>;
}
impl Append for File {
  fn append(&self, buf: &[u8]) -> Result<usize> {
    let _lock = FLock::new(self)?;
    let len = self.metadata()?.len();
    self.pwrite(buf, len)?;
    Ok(len as usize)
  }
}

struct FLock<'a>(&'a File);
impl<'a> FLock<'a> {
  fn new(file: &'a File) -> Result<Self> {
    fs2::FileExt::lock_exclusive(file)?;
    Ok(FLock(file))
  }
}
impl<'a> Drop for FLock<'a> {
  fn drop(&mut self) {
    let _ = fs2::FileExt::unlock(self.0);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::fs::File;
  use std::io::{Read, Write};
  use tempfile::tempdir;

  #[test]
  fn test_pread() -> Result<()> {
    let dir = tempdir()?;
    let file_path = dir.path().join("test_file.txt");
    let content = b"Hello, World!";

    // Create a test file with content
    let mut file = File::create(&file_path)?;
    file.write_all(content)?;
    file.sync_all()?;

    // Open file for reading
    let file = File::open(&file_path)?;

    // Test 1: Normal read
    let mut buf = vec![0; 5];
    let bytes_read = file.pread(&mut buf, 0)?;
    assert_eq!(bytes_read, 5);
    assert_eq!(&buf, b"Hello");

    // Test 2: Read from middle
    let mut buf = vec![0; 5];
    let bytes_read = file.pread(&mut buf, 7)?;
    assert_eq!(bytes_read, 5);
    assert_eq!(&buf, b"World");

    // Test 3: Read beyond file size
    let mut buf = vec![0; 5];
    let bytes_read = file.pread(&mut buf, 20)?;
    assert_eq!(bytes_read, 0);

    // Test 4: Read with empty buffer
    let mut buf = vec![];
    let bytes_read = file.pread(&mut buf, 0)?;
    assert_eq!(bytes_read, 0);

    Ok(())
  }

  #[test]
  fn test_pwrite() -> Result<()> {
    let dir = tempdir()?;
    let file_path = dir.path().join("test_pwrite.txt");

    // Create an empty file
    let file = File::create(&file_path)?;

    // Test 1: Write at the beginning
    let content1 = b"Hello";
    let bytes_written = file.pwrite(content1, 0)?;
    assert_eq!(bytes_written, 5);

    // Test 2: Write at specific offset
    let content2 = b"World";
    let bytes_written = file.pwrite(content2, 6)?;
    assert_eq!(bytes_written, 5);

    // Verify written content
    let mut content = String::new();
    File::open(&file_path)?.read_to_string(&mut content)?;
    assert_eq!(content, "Hello\0World");

    // Test 3: Write with empty buffer
    let empty_buf: &[u8] = &[];
    let bytes_written = file.pwrite(empty_buf, 0)?;
    assert_eq!(bytes_written, 0);

    Ok(())
  }
}
