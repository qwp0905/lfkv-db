use std::{
  fs::{File, OpenOptions},
  io::Result,
};

#[cfg(unix)]
use std::os::unix::fs::{FileExt, OpenOptionsExt};

#[cfg(unix)]
use libc;

#[cfg(windows)]
use std::os::windows::fs::{FileExt, OpenOptionsExt};
#[cfg(windows)]
use winapi;

pub trait Pread {
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
}
impl Pread for File {
  #[cfg(unix)]
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    self.read_at(buf, offset)
  }

  #[cfg(windows)]
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

pub trait DirectIO {
  fn direct_io(&mut self) -> &mut Self;
}
impl DirectIO for OpenOptions {
  #[cfg(target_os = "macos")]
  fn direct_io(&mut self) -> &mut Self {
    self.custom_flags(libc::F_NOCACHE)
  }
  #[cfg(all(unix, not(target_os = "macos")))]
  fn direct_io(&mut self) -> &mut Self {
    self.custom_flags(libc::O_DIRECT)
  }
  #[cfg(windows)]
  fn direct_io(&mut self) -> &mut Self {
    self.custom_flags(winapi::FILE_FLAG_NO_BUFFERING)
  }
}

#[cfg(test)]
#[path = "tests/io.rs"]
mod tests;
