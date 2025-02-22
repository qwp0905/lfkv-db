use std::fs::{File, OpenOptions};
use std::io::Result;
#[cfg(any(target_os = "macos", target_os = "linux"))]
use std::os::unix::fs::FileExt;
#[cfg(target_os = "windows")]
use std::os::windows::fs::FileExt;
use std::path::Path;

pub trait DirectIO {
  fn direct_io<P: AsRef<Path>>(&self, path: P) -> Result<File>;
}
#[cfg(target_os = "macos")]
use std::os::unix::io::AsRawFd;

impl DirectIO for OpenOptions {
  fn direct_io<P: AsRef<Path>>(&self, path: P) -> Result<File> {
    let file = self.open(path)?;
    let fd = file.as_raw_fd();
    unsafe {
      libc::fcntl(fd, libc::F_NOCACHE, 1);
    }
    Ok(file)
  }
}
#[cfg(target_os = "linux")]
impl DirectIO for OpenOptions {
  fn direct_io<P: AsRef<Path>>(&self, path: P) -> Result<File> {
    self.custom_flags(libc::O_DIRECT).open(path)
  }
}
#[cfg(target_os = "windows")]
impl DirectIO for OpenOptions {
  fn direct_io<P: AsRef<Path>>(&self, path: P) -> Result<File> {
    self
      .custom_flags(winapi::um::winbase::FILE_FLAG_NO_BUFFERING)
      .open(path)
  }
}

pub trait OffsetRead {
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
impl OffsetRead for File {
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    self.read_at(buf, offset)
  }
}
#[cfg(target_os = "windows")]
impl OffsetRead for File {
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    self.seek_read(buf, offset)
  }
}

pub trait OffsetWrite {
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize>;
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
impl OffsetWrite for File {
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
    self.write_at(buf, offset)
  }
}
#[cfg(target_os = "windows")]
impl OffsetWrite for File {
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
    self.seek_write(buf, offset)
  }
}
