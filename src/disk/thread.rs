use std::{
  fs::{File, Metadata},
  ops::Mul,
  path::PathBuf,
};

use crate::{
  disk::PageRef, Error, Page, Result, SafeWork, SingleWorkThread, WorkBuilder,
};

use super::{Pread, Pwrite};

pub fn create_read_thread<'a, const N: usize>(
  file: &'a File,
) -> impl Fn(usize) -> Result<SafeWork<(usize, PageRef<N>), std::io::Result<PageRef<N>>>>
     + use<'a, N> {
  |_| {
    let fd = file.try_clone().map_err(Error::IO)?;
    let work = SafeWork::no_timeout(move |(index, mut page): (usize, PageRef<N>)| {
      fd.pread(page.as_mut().as_mut(), index.mul(N) as u64)?;
      Ok(page)
    });
    Ok(work)
  }
}

pub fn create_write_thread<'a, const N: usize>(
  file: &'a File,
) -> impl Fn(usize) -> Result<SafeWork<(usize, Page<N>), std::io::Result<()>>> + use<'a, N>
{
  |_| {
    let fd = file.try_clone().map_err(Error::IO)?;
    let work = SafeWork::no_timeout(move |(index, page): (usize, Page<N>)| {
      fd.pwrite(page.as_ref(), index.mul(N) as u64)?;
      Ok(())
    });
    Ok(work)
  }
}

pub fn create_flush_thread<'a>(
  file: &'a File,
  path: &'a PathBuf,
) -> Result<SingleWorkThread<(), std::io::Result<()>>> {
  let fd = file.try_clone().map_err(Error::IO)?;
  Ok(
    WorkBuilder::new()
      .name(format!("flush {}", path.to_string_lossy()))
      .stack_size(2 << 10)
      .single()
      .no_timeout(move |_| fd.sync_all()),
  )
}

pub fn create_metadata_thread<'a>(
  file: &'a File,
  path: &'a PathBuf,
) -> Result<SingleWorkThread<(), std::io::Result<Metadata>>> {
  let fd = file.try_clone().map_err(Error::IO)?;
  Ok(
    WorkBuilder::new()
      .name(format!("flush {}", path.to_string_lossy()))
      .stack_size(2 << 10)
      .single()
      .no_timeout(move |_| fd.metadata()),
  )
}
