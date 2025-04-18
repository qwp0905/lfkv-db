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
