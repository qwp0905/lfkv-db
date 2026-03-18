use std::{
  fs::{File, OpenOptions},
  io::{IoSlice, Result},
};

#[cfg(unix)]
use std::io::Error;
#[cfg(unix)]
use std::os::{
  fd::AsRawFd,
  unix::fs::{FileExt, OpenOptionsExt},
};

#[cfg(unix)]
use libc;

#[cfg(windows)]
use std::{
  intrinsics::copy_nonoverlapping,
  os::windows::fs::{FileExt, OpenOptionsExt},
};
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

pub trait Pwritev {
  fn pwritev(&self, bufs: &[IoSlice], offset: u64) -> Result<usize>;
}
impl Pwritev for File {
  #[cfg(unix)]
  fn pwritev(&self, bufs: &[IoSlice], offset: u64) -> Result<usize> {
    let ret = unsafe {
      libc::pwritev(
        self.as_raw_fd(),
        bufs.as_ptr() as *const libc::iovec,
        bufs.len().min(libc::IOV_MAX as usize) as libc::c_int,
        offset as _,
      )
    };
    if ret == -1 {
      return Err(Error::last_os_error());
    }

    Ok(ret as usize)
  }
  #[cfg(windows)]
  fn pwritev(&self, bufs: &[IoSlice], offset: u64) -> Result<usize> {
    let total: usize = bufs.iter().map(|b| b.len()).sum();
    let mut buf = vec![0u8; total];
    let ptr = buf.as_mut_ptr();
    let mut pos = 0;
    for slice in bufs {
      unsafe { copy_nonoverlapping(slice.as_ptr(), ptr.add(pos), slice.len()) };
      pos += slice.len();
    }
    self.seek_write(&buf, offset)
  }
}

pub trait DirectIO {
  fn direct_io(&mut self) -> &mut Self;
}
impl DirectIO for OpenOptions {
  #[cfg(target_vendor = "apple")]
  fn direct_io(&mut self) -> &mut Self {
    self.custom_flags(libc::F_NOCACHE)
  }
  #[cfg(all(unix, not(target_vendor = "apple")))]
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
