use std::{
  fs::{File, OpenOptions},
  io::{IoSlice, /* IoSliceMut,  */ Result},
  path::Path,
};

#[cfg(unix)]
use libc;

#[cfg(unix)]
use std::{
  io::Error,
  os::{fd::AsRawFd, unix::fs::FileExt},
};

#[cfg(windows)]
use winapi;

#[cfg(windows)]
use std::{
  intrinsics::copy_nonoverlapping,
  os::windows::fs::{FileExt, OpenOptionsExt},
};

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

// pub trait Preadv {
//   fn preadv(&self, bufs: &mut [IoSliceMut], offset: u64) -> Result<usize>;
// }
// impl Preadv for File {
//   #[cfg(unix)]
//   fn preadv(&self, bufs: &mut [IoSliceMut], offset: u64) -> Result<usize> {
//     let ret = unsafe {
//       libc::preadv(
//         self.as_raw_fd(),
//         bufs.as_mut_ptr() as *mut libc::iovec as *const libc::iovec,
//         bufs.len() as libc::c_int,
//         offset as _,
//       )
//     };
//     if ret == -1 {
//       return Err(Error::last_os_error());
//     }

//     Ok(ret as usize)
//   }
//   #[cfg(windows)]
//   fn preadv(&self, bufs: &mut [IoSliceMut], offset: u64) -> Result<usize> {
//     let total: usize = bufs.iter().map(|b| b.len()).sum();
//     let mut buf = vec![0u8; total];
//     let ret = self.seek_read(&mut buf, offset)?;
//     let mut pos = 0;
//     let ptr = buf.as_ptr();
//     for slice in bufs {
//       unsafe { copy_nonoverlapping(ptr.add(pos), slice.as_mut_ptr(), slice.len()) };
//       pos += slice.len();
//     }
//     Ok(ret)
//   }
// }

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
        bufs.len() as libc::c_int,
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
  fn direct_io<P: AsRef<Path>>(&self, path: P) -> Result<File>;
}

impl DirectIO for OpenOptions {
  #[cfg(target_vendor = "apple")]
  fn direct_io<P: AsRef<Path>>(&self, path: P) -> Result<File> {
    let file = self.open(path)?;
    let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
    if ret == -1 {
      return Err(Error::last_os_error());
    }
    Ok(file)
  }
  #[cfg(all(unix, not(target_vendor = "apple")))]
  fn direct_io<P: AsRef<Path>>(&self, path: P) -> Result<File> {
    self.custom_flags(libc::O_DIRECT).open(path)
  }
  #[cfg(windows)]
  fn direct_io<P: AsRef<Path>>(&self, path: P) -> Result<File> {
    self
      .custom_flags(winapi::um::winbase::FILE_FLAG_NO_BUFFERING)
      .open(path)
  }
}

#[cfg(test)]
#[path = "tests/io.rs"]
mod tests;
