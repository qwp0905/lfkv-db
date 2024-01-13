use std::{
  fs::{remove_file, File, OpenOptions},
  io::{Read, Seek, SeekFrom, Write},
  path::{Path, PathBuf},
  sync::RwLock,
};

use utils::ShortenedRwLock;

use crate::error::{Error, Result};

use super::{Page, PAGE_SIZE};

#[derive(Debug)]
pub struct PageSeeker {
  inner: RwLock<File>,
  path: PathBuf,
}
impl PageSeeker {
  pub fn open<T>(path: T) -> Result<PageSeeker>
  where
    T: AsRef<Path>,
  {
    return OpenOptions::new()
      .create(true)
      .read(true)
      .write(true)
      .open(path.as_ref())
      .map(|inner| Self {
        inner: RwLock::new(inner),
        path: PathBuf::from(path.as_ref()),
      })
      .map_err(Error::IO);
  }

  pub fn read(&self, index: usize) -> Result<Page> {
    let mut inner = self.inner.wl();
    inner
      .seek(SeekFrom::Start(get_offset(index)))
      .map_err(Error::IO)?;
    let mut page = Page::new();
    inner
      .read_exact(page.as_mut())
      .map_err(|_| Error::NotFound)?;
    return Ok(page);
  }

  pub fn write(&self, index: usize, page: Page) -> Result<()> {
    let mut inner = self.inner.wl();
    inner
      .seek(SeekFrom::Start(get_offset(index)))
      .map_err(Error::IO)?;
    inner.write_all(page.as_ref()).map_err(Error::IO)?;
    return Ok(());
  }

  pub fn append(&self, page: Page) -> Result<usize> {
    let mut inner = self.inner.wl();
    let n = inner.seek(SeekFrom::End(0)).map_err(Error::IO)?;
    inner.write_all(page.as_ref()).map_err(Error::IO)?;
    return Ok(n as usize / PAGE_SIZE);
  }

  pub fn fsync(&self) -> Result<()> {
    return self.inner.rl().sync_all().map_err(Error::IO);
  }

  pub fn truncate(&self, size: usize) -> Result<()> {
    return self.inner.wl().set_len(get_offset(size)).map_err(Error::IO);
  }

  pub fn delete(self) -> Result<()> {
    drop(self.inner);
    remove_file(self.path).map_err(Error::IO)
  }

  pub fn len(&self) -> Result<usize> {
    let metadata = self.inner.rl().metadata().map_err(Error::IO)?;
    Ok(metadata.len() as usize)
  }
}
unsafe impl Send for PageSeeker {}

fn get_offset(index: usize) -> u64 {
  (index * PAGE_SIZE) as u64
}
