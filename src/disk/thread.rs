use std::{
  fs::{File, Metadata},
  ops::{Add, Mul},
  path::PathBuf,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
};

use crate::{Error, Result, SafeWork, SingleWorkThread, WorkBuilder};

use super::{Page, Pread, Pwrite};

pub fn create_read_thread<'a, const N: usize>(
  file: &'a File,
) -> impl Fn(usize) -> Result<SafeWork<usize, std::io::Result<Page<N>>>> + use<'a, N> {
  |_| {
    let fd = file.try_clone().map_err(Error::IO)?;
    let work = SafeWork::no_timeout(move |index: usize| {
      let mut page = Page::new();
      fd.pread(page.as_mut(), index.mul(N) as u64)?;
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

pub fn create_append_thread<'a, const N: usize>(
  file: &'a File,
  counter: &'a Arc<AtomicUsize>,
  max_size: usize,
) -> impl Fn(usize) -> Result<SafeWork<Page<N>, std::io::Result<usize>>> + use<'a, N> {
  move |_| {
    let fd = file.try_clone().map_err(Error::IO)?;
    let counter = counter.clone();
    let work = SafeWork::no_timeout(move |page: Page<N>| {
      let index = counter
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |counter| {
          Some(counter.add(1).rem_euclid(max_size))
        })
        .unwrap();
      fd.pwrite(page.as_ref(), index.mul(N) as u64)?;
      Ok(index)
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
