use std::{
  fs::{Metadata, OpenOptions},
  path::PathBuf,
  sync::Arc,
};

use super::{DirectIO, PagePool, PageRef, Pread, Pwrite};
use crate::{
  error::{Error, Result},
  thread::{SharedWorkThread, WorkBuilder, WorkResult},
  utils::ToArc,
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
  pub fn wait(self) -> Result {
    self.0.wait()?.map_err(Error::IO)?;
    Ok(())
  }
}

pub struct DiskController<const N: usize> {
  background:
    Arc<SharedWorkThread<DiskOperation<N>, std::io::Result<OperationResult<N>>>>,
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
      .shared(config.thread_count)
      .build(move |_| {
        let fd = file.try_clone().map_err(Error::IO)?;
        let work = move |operation: DiskOperation<N>| match operation {
          DiskOperation::Read(offset, mut page) => fd
            .pread(page.as_mut().as_mut(), offset)
            .map(|_| OperationResult::Read(page)),
          DiskOperation::Write(offset, page) => fd
            .pwrite(page.as_ref().as_ref(), offset)
            .map(|_| OperationResult::Write),
          DiskOperation::Flush => fd.sync_all().map(|_| OperationResult::Flush),
          DiskOperation::Metadata => Ok(OperationResult::Metadata(fd.metadata()?)),
        };
        Ok(work)
      })?
      .to_arc();

    Ok(Self {
      background,
      page_pool,
    })
  }

  pub fn read(&self, index: usize) -> Result<PageRef<N>> {
    Ok(
      self
        .background
        .send_await(DiskOperation::Read(
          (index * N) as u64,
          self.page_pool.acquire(),
        ))?
        .map_err(Error::IO)?
        .as_read(),
    )
  }

  pub fn write<'a>(&self, index: usize, page: &'a PageRef<N>) -> Result {
    self.write_async(index, page).wait()
  }
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
