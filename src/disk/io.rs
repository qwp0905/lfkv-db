use std::{
  fs::File,
  io::Result,
  os::fd::{AsRawFd, FromRawFd, RawFd},
};

#[cfg(any(target_os = "macos", target_os = "linux"))]
use std::os::unix::fs::FileExt;

#[cfg(target_os = "windows")]
use std::{
  io::Error,
  mem,
  os::windows::{
    fs::FileExt,
    io::{AsRawHandle, FromRawHandle, RawHandle},
  },
};
#[cfg(target_os = "windows")]
use winapi::shared::{
  minwindef::{BOOL, DWORD},
  um::{
    fileapi::{
      GetDiskFreeSpaceW, GetVolumePathNameW, LockFileEx, SetFileInformationByHandle,
      UnlockFile, FILE_ALLOCATION_INFO, FILE_STANDARD_INFO,
    },
    handleapi::DuplicateHandle,
    minwinbase::{
      FileAllocationInfo, FileStandardInfo, LOCKFILE_EXCLUSIVE_LOCK,
      LOCKFILE_FAIL_IMMEDIATELY,
    },
    processthreadsapi::GetCurrentProcess,
    winbase::GetFileInformationByHandleEx,
  },
  winerror::ERROR_LOCK_VIOLATION,
  winnt::DUPLICATE_SAME_ACCESS,
};

pub trait CopyableFile {
  fn copy(&self) -> Result<Self>
  where
    Self: Sized;
}
impl CopyableFile for File {
  #[cfg(any(target_os = "macos", target_os = "linux"))]
  fn copy(&self) -> Result<Self>
  where
    Self: Sized,
  {
    let fd = self.as_raw_fd();
    Ok(unsafe { File::from_raw_fd(libc::dup(fd)) })
  }

  #[cfg(target_os = "windows")]
  fn copy(&self) -> Result<Self>
  where
    Self: Sized,
  {
    let mut handle = ptr::null_mut();
    let current_process = GetCurrentProcess();
    let ret = DuplicateHandle(
      current_process,
      self.as_raw_handle(),
      current_process,
      &mut handle,
      0,
      true as BOOL,
      DUPLICATE_SAME_ACCESS,
    );
    if ret == 0 {
      return Err(Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_handle(handle) })
  }
}
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

struct FLock(RawFd);
impl FLock {
  #[cfg(any(target_os = "macos", target_os = "linux"))]
  pub fn new<T: AsRawFd>(file: &T) -> Result<Self> {
    let fd = file.as_raw_fd();
    unsafe {
      libc::flock(fd, libc::LOCK_EX);
    }
    Ok(Self(fd))
  }
  #[cfg(target_os = "windows")]
  pub fn new(file: &File) -> Result<Self> {
    let fd = file.as_raw_handle();
    unsafe {
      let mut overlapped = mem::zeroed();
      let ret = LockFileEx(fd, LOCKFILE_EXCLUSIVE_LOCK, 0, !0, !0, &mut overlapped);
      if ret == 0 {
        return Err(Error::last_os_error());
      }
    }
    Ok(Self(fd))
  }
  #[cfg(any(target_os = "macos", target_os = "linux"))]
  pub fn release(&self) -> Result<()> {
    unsafe { libc::flock(self.0, libc::LOCK_UN) };
    Ok(())
  }

  #[cfg(target_os = "windows")]
  pub fn release(&self) -> Result<()> {
    if UnlockFile(self.0, 0, 0, !0, !0) == 0 {
      return Err(Error::last_os_error());
    }
    Ok(())
  }
}
impl Drop for FLock {
  fn drop(&mut self) {
    let _ = self.release();
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
