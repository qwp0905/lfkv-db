use std::fs::{File, OpenOptions};
use std::io::Result;
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
