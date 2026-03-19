use std::{
  fs::{File, Metadata, OpenOptions},
  path::PathBuf,
  sync::Arc,
};

use super::{DirectIO, PagePool, PageRef, Pread, Pwrite};
use crate::{
  error::{Error, Result},
  thread::{BackgroundThread, WorkBuilder, WorkResult},
  utils::ToBox,
};

enum DiskOperation<const N: usize> {
  Read(u64, PageRef<N>),
  Write(u64, PageRef<N>),
  Flush,
  Metadata,
}
enum OperationResult<const N: usize> {
  Read(PageRef<N>),
  Write,
  Flush,
  Metadata(Metadata),
}
impl<const N: usize> OperationResult<N> {
  fn as_read(self) -> PageRef<N> {
    match self {
      OperationResult::Read(page) => page,
      _ => unreachable!(),
    }
  }
  fn as_meta(self) -> Metadata {
    match self {
      OperationResult::Metadata(meta) => meta,
      _ => unreachable!(),
    }
  }
}

pub struct DiskControllerConfig {
  pub path: PathBuf,
  pub thread_count: usize,
}

pub struct WriteAsync<const N: usize>(WorkResult<std::io::Result<OperationResult<N>>>);
impl<const N: usize> WriteAsync<N> {
  #[inline]
  pub fn wait(self) -> Result {
    self.0.wait()?.map_err(Error::IO)?;
    Ok(())
  }
}
pub struct ReadAsync<const N: usize>(WorkResult<std::io::Result<OperationResult<N>>>);
impl<const N: usize> ReadAsync<N> {
  #[inline]
  pub fn wait(self) -> Result<PageRef<N>> {
    Ok(self.0.wait()?.map_err(Error::IO)?.as_read())
  }
}

fn handle_disk<const N: usize>(
  file: File,
) -> impl Fn(DiskOperation<N>) -> std::io::Result<OperationResult<N>> {
  move |operation: DiskOperation<N>| match operation {
    DiskOperation::Read(offset, mut page) => file
      .pread(page.as_mut().as_mut(), offset)
      .map(|_| OperationResult::Read(page)),
    DiskOperation::Write(offset, page) => file
      .pwrite(page.as_ref().as_ref(), offset)
      .map(|_| OperationResult::Write),
    DiskOperation::Flush => file.sync_all().map(|_| OperationResult::Flush),
    DiskOperation::Metadata => Ok(OperationResult::Metadata(file.metadata()?)),
  }
}

pub struct DiskController<const N: usize> {
  background:
    Box<dyn BackgroundThread<DiskOperation<N>, std::io::Result<OperationResult<N>>>>,
  page_pool: Arc<PagePool<N>>,
}
impl<const N: usize> DiskController<N> {
  pub fn open(config: DiskControllerConfig, page_pool: Arc<PagePool<N>>) -> Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .direct_io()
      .open(&config.path)
      .map_err(Error::IO)?;

    let background = WorkBuilder::new()
      .name(format!("disk {}", config.path.to_string_lossy()))
      .stack_size(N * 500)
      .multi(config.thread_count)
      .stealing(handle_disk(file))
      .to_box();

    Ok(Self {
      background,
      page_pool,
    })
  }

  pub fn read(&self, index: usize) -> Result<PageRef<N>> {
    self.read_async(index).wait()
  }
  #[inline]
  pub fn read_async(&self, index: usize) -> ReadAsync<N> {
    let done = self.background.send(DiskOperation::Read(
      (index * N) as u64,
      self.page_pool.acquire(),
    ));
    ReadAsync(done)
  }

  pub fn write<'a>(&self, index: usize, page: &'a PageRef<N>) -> Result {
    self.write_async(index, page).wait()
  }
  #[inline]
  pub fn write_async<'a>(&self, index: usize, page: &'a PageRef<N>) -> WriteAsync<N> {
    let mut pooled = self.page_pool.acquire();
    pooled.as_mut().copy_from(page.as_ref());
    let o = self
      .background
      .send(DiskOperation::Write((index * N) as u64, pooled));
    WriteAsync(o)
  }

  pub fn fsync(&self) -> Result {
    self
      .background
      .send_await(DiskOperation::Flush)?
      .map_err(Error::IO)?;
    Ok(())
  }

  pub fn close(&self) {
    self.background.close();
  }

  pub fn len(&self) -> Result<usize> {
    let meta = self
      .background
      .send_await(DiskOperation::Metadata)?
      .map_err(Error::IO)?
      .as_meta();
    Ok((meta.len() as usize) / N)
  }
}

#[cfg(test)]
#[path = "tests/controller.rs"]
mod tests;
