use std::{
  fs::{File, OpenOptions},
  io::{Read, Seek, SeekFrom, Write},
  path::Path,
  sync::RwLock,
};

use utils::RwLocker;

use super::{Page, PAGE_SIZE};

#[derive(Debug)]
pub struct PageSeeker {
  inner: RwLock<File>,
}
impl PageSeeker {
  pub fn open<T>(path: T) -> std::io::Result<PageSeeker>
  where
    T: AsRef<Path>,
  {
    return OpenOptions::new()
      .create(true)
      .read(true)
      .write(true)
      .open(path)
      .map(|inner| Self {
        inner: RwLock::new(inner),
      });
  }

  pub fn get(&self, index: usize) -> std::io::Result<Page> {
    let mut inner = self.inner.wl();
    inner.seek(SeekFrom::Start(get_offset(index)))?;
    let mut page = Page::new();
    inner.read_exact(page.as_mut())?;
    return Ok(page);
  }

  pub fn write(&self, index: usize, page: Page) -> std::io::Result<()> {
    let mut inner = self.inner.wl();
    inner.seek(SeekFrom::Start(get_offset(index)))?;
    inner.write_all(page.as_ref())?;
    return Ok(());
  }

  pub fn append(&self, page: Page) -> std::io::Result<usize> {
    let mut inner = self.inner.wl();
    let n = inner.seek(SeekFrom::End(0))?;
    inner.write_all(page.as_ref())?;
    return Ok(n as usize / PAGE_SIZE);
  }

  pub fn fsync(&self) -> std::io::Result<()> {
    return self.inner.rl().sync_all();
  }

  pub fn truncate(&self, size: usize) -> std::io::Result<()> {
    return self.inner.wl().set_len(get_offset(size));
  }

  pub fn len(&self) -> std::io::Result<usize> {
    let metadata = self.inner.rl().metadata()?;
    Ok(metadata.len() as usize)
  }
}
unsafe impl Send for PageSeeker {}

fn get_offset(index: usize) -> u64 {
  (index * PAGE_SIZE) as u64
}
