use std::fs::{File, OpenOptions};
use std::io::Result;
use std::os::unix::io::AsRawFd;
#[cfg(target_os = "windows")]
use std::os::windows::io::{AsRawHandle, RawHandle};
use std::path::Path;

#[cfg(target_os = "linux")]
pub fn open<P>(path: P) -> Result<File>
where
  P: AsRef<Path>,
{
  use std::os::unix::fs::OpenOptionsExt;
  OpenOptions::new()
    .create(true)
    .read(true)
    .write(true)
    .custom_flags(libc::O_DIRECT)
    .open(path)
}

#[cfg(target_os = "macos")]
pub fn open<P>(path: P) -> Result<File>
where
  P: AsRef<Path>,
{
  let file = OpenOptions::new()
    .create(true)
    .read(true)
    .write(true)
    .open(path)?;
  let fd = file.as_raw_fd();
  unsafe {
    libc::fcntl(fd, libc::F_NOCACHE, 1);
  }
  Ok(file)
}

#[cfg(target_os = "windows")]
pub fn open<P>(path: P) -> Result<File>
where
  P: AsRef<Path>,
{
  use std::os::windows::fs::OpenOptionsExt;
  OpenOptions::new()
    .create(true)
    .read(true)
    .write(true)
    .custom_flags(winapi::um::winbase::FILE_FLAG_NO_BUFFERING)
    .open(path)
}
