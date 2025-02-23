use std::fs::{File, Metadata, OpenOptions};
use std::io::Result;
use std::os::fd::FromRawFd;
#[cfg(any(target_os = "macos", target_os = "linux"))]
use std::os::unix::fs::FileExt;
#[cfg(target_os = "macos")]
use std::os::unix::io::AsRawFd;
#[cfg(target_os = "windows")]
use std::os::windows::fs::FileExt;
use std::path::Path;

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
    use std::os::windows::io::{AsRawHandle, FromRawHandle, RawHandle};
    use winapi::shared::minwindef::FALSE;
    use winapi::um::handleapi::DuplicateHandle;
    use winapi::um::processthreadsapi::GetCurrentProcess;
    use winapi::um::winnt::HANDLE;
    let handle = self.0.as_raw_handle();
    let mut new_handle: HANDLE = ptr::null_mut();
    unsafe {
      DuplicateHandle(
        GetCurrentProcess(),
        handle,
        GetCurrentProcess(),
        &mut new_handle,
        0,
        FALSE,
        DUPLICATE_SAME_ACCESS,
      )?;
    }

    Ok(Self(unsafe { File::from_raw_handle(new_handle) }))
  }

  #[cfg(any(target_os = "macos", target_os = "linux"))]
  pub fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    self.0.read_at(buf, offset)
  }

  #[cfg(target_os = "windows")]
  pub fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    self.0.seek_read(buf, offset)
  }

  #[cfg(any(target_os = "macos", target_os = "linux"))]
  pub fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    self.0.write_at(buf, offset)
  }

  #[cfg(target_os = "windows")]
  pub fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    self.0.seek_write(buf, offset)
  }

  pub fn fsync(&self) -> std::io::Result<()> {
    self.0.sync_all()
  }

  pub fn metadata(&self) -> std::io::Result<Metadata> {
    self.0.metadata()
  }
}
