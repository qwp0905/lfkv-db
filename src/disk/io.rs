use std::{fs::File, io::Result};

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

pub trait Pread {
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
}
impl Pread for File {
  #[cfg(unix)]
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
  #[cfg(unix)]
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
    self.write_at(buf, offset)
  }

  #[cfg(windows)]
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
    self.seek_write(buf, offset)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::fs::{File /* OpenOptions */};
  use std::io::{Read, Write};
  // use std::thread;
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
