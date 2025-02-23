use std::fs::{File, Metadata, OpenOptions};
#[cfg(target_os = "windows")]
use std::io::Error;
use std::io::Result;
#[cfg(target_os = "windows")]
use std::mem;
use std::os::fd::{FromRawFd, RawFd};
#[cfg(any(target_os = "macos", target_os = "linux"))]
use std::os::unix::fs::FileExt;
#[cfg(target_os = "macos")]
use std::os::unix::io::AsRawFd;
#[cfg(target_os = "windows")]
#[cfg(target_os = "windows")]
use std::os::windows::fs::FileExt;
#[cfg(target_os = "windows")]
use std::os::windows::io::{AsRawHandle, FromRawHandle, RawHandle};
use std::path::Path;
#[cfg(target_os = "windows")]
use winapi::shared::minwindef::{BOOL, DWORD};
#[cfg(target_os = "windows")]
use winapi::shared::winerror::ERROR_LOCK_VIOLATION;
#[cfg(target_os = "windows")]
use winapi::um::fileapi::{GetDiskFreeSpaceW, FILE_ALLOCATION_INFO, FILE_STANDARD_INFO};
#[cfg(target_os = "windows")]
use winapi::um::fileapi::{
  GetVolumePathNameW, LockFileEx, SetFileInformationByHandle, UnlockFile,
};
#[cfg(target_os = "windows")]
use winapi::um::handleapi::DuplicateHandle;
#[cfg(target_os = "windows")]
use winapi::um::minwinbase::{FileAllocationInfo, FileStandardInfo};
#[cfg(target_os = "windows")]
use winapi::um::minwinbase::{LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY};
#[cfg(target_os = "windows")]
use winapi::um::processthreadsapi::GetCurrentProcess;
#[cfg(target_os = "windows")]
use winapi::um::winbase::GetFileInformationByHandleEx;
#[cfg(target_os = "windows")]
use winapi::um::winnt::DUPLICATE_SAME_ACCESS;

pub trait DirectIO {
  fn direct_io<P: AsRef<Path>>(&self, path: P) -> Result<CopyableFile>;
}

impl DirectIO for OpenOptions {
  fn direct_io<P: AsRef<Path>>(&self, path: P) -> Result<CopyableFile> {
    let file = self.open(path)?;
    let fd = file.as_raw_fd();
    unsafe {
      libc::fcntl(fd, libc::F_NOCACHE, 1);
    }
    Ok(CopyableFile(file))
  }
}
#[cfg(target_os = "linux")]
impl DirectIO for OpenOptions {
  fn direct_io<P: AsRef<Path>>(&self, path: P) -> Result<File> {
    self
      .custom_flags(libc::O_DIRECT)
      .open(path)
      .map(CopyableFile)
  }
}
#[cfg(target_os = "windows")]
impl DirectIO for OpenOptions {
  fn direct_io<P: AsRef<Path>>(&self, path: P) -> Result<File> {
    self
      .custom_flags(winapi::um::winbase::FILE_FLAG_NO_BUFFERING)
      .open(path)
      .map(CopyableFile)
  }
}

pub struct CopyableFile(File);
impl CopyableFile {
  #[cfg(any(target_os = "macos", target_os = "linux"))]
  pub fn copy(&self) -> std::io::Result<Self> {
    let fd = self.0.as_raw_fd();
    Ok(Self(unsafe { File::from_raw_fd(libc::dup(fd)) }))
  }

  #[cfg(target_os = "windows")]
  pub fn copy(&self) -> std::io::Result<Self> {
    let mut handle = ptr::null_mut();
    let current_process = GetCurrentProcess();
    let ret = DuplicateHandle(
      current_process,
      file.as_raw_handle(),
      current_process,
      &mut handle,
      0,
      true as BOOL,
      DUPLICATE_SAME_ACCESS,
    );
    if ret == 0 {
      return Err(Error::last_os_error());
    }
    Ok(Self(unsafe { File::from_raw_handle(handle) }))
  }

  #[cfg(any(target_os = "macos", target_os = "linux"))]
  pub fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    self.0.read_at(buf, offset)
  }

  #[cfg(target_os = "windows")]
  pub fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    self.0.seek_read(buf, offset)
  }

  #[cfg(any(target_os = "macos", target_os = "linux"))]
  pub fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
    self.0.write_at(buf, offset)
  }

  #[cfg(target_os = "windows")]
  pub fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
    self.0.seek_write(buf, offset)
  }

  pub fn fsync(&self) -> Result<()> {
    self.0.sync_all()
  }

  pub fn metadata(&self) -> Result<Metadata> {
    self.0.metadata()
  }

  #[allow(unused)]
  pub fn append(&self, buf: &[u8]) -> Result<usize> {
    let _ = FLock::new(&self.0)?;
    let len = self.0.metadata()?.len();
    self.pwrite(buf, len)?;
    Ok(len as usize)
  }
}

struct FLock(RawFd);
impl FLock {
  #[cfg(any(target_os = "macos", target_os = "linux"))]
  pub fn new(file: &File) -> Result<Self> {
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
    self.release().ok();
  }
}
