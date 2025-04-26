use std::{
  fs::{Metadata, OpenOptions},
  ops::{Div, Mul},
  path::PathBuf,
  sync::{atomic::AtomicUsize, Arc},
};

use crate::{Error, Result, SharedWorkThread, SingleWorkThread, ToArc, WorkBuilder};

use super::{
  thread::{
    create_append_thread, create_flush_thread, create_metadata_thread, create_read_thread,
  },
  Page,
};

pub struct SequentialAccessDiskConfig {
  pub path: PathBuf,
  pub read_threads: Option<usize>,
  pub append_threads: Option<usize>,
  pub max_file_size: usize,
}

const DEFAULT_READ_THREADS: usize = 1;
const DEFAULT_WRITE_THREADS: usize = 3;

pub struct SequentialAccessDisk<const N: usize> {
  read_ths: Arc<SharedWorkThread<usize, std::io::Result<Page<N>>>>,
  append_ths: Arc<SharedWorkThread<Page<N>, std::io::Result<usize>>>,
  flush_th: Arc<SingleWorkThread<(), std::io::Result<()>>>,
  meta_th: Arc<SingleWorkThread<(), std::io::Result<Metadata>>>,
}
impl<const N: usize> SequentialAccessDisk<N> {
  pub fn open(config: SequentialAccessDiskConfig) -> Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(&config.path)
      .map_err(Error::IO)?;

    let read_ths = WorkBuilder::new()
      .name(format!("read {}", config.path.to_string_lossy()))
      .stack_size(N.mul(150))
      .shared(config.read_threads.unwrap_or(DEFAULT_READ_THREADS))
      .build(create_read_thread(&file))?
      .to_arc();

    let counter =
      AtomicUsize::new(file.metadata().map_err(Error::IO)?.len() as usize).to_arc();
    let append_ths = WorkBuilder::new()
      .name(format!("append {}", config.path.to_string_lossy()))
      .stack_size(N.mul(150))
      .shared(config.append_threads.unwrap_or(DEFAULT_WRITE_THREADS))
      .build(create_append_thread(&file, &counter, config.max_file_size))?
      .to_arc();

    Ok(Self {
      read_ths,
      append_ths,
      flush_th: create_flush_thread(&file, &config.path)?.to_arc(),
      meta_th: create_metadata_thread(&file, &config.path)?.to_arc(),
    })
  }

  pub fn read(&self, index: usize) -> Result<Page<N>> {
    self.read_ths.send_await(index)?.map_err(Error::IO)
  }

  pub fn append(&self, page: Page<N>) -> Result<usize> {
    self.append_ths.send_await(page)?.map_err(Error::IO)
  }

  pub fn fsync(&self) -> Result {
    self.flush_th.send_await(())?.map_err(Error::IO)
  }

  pub fn len(&self) -> Result<usize> {
    let meta = self.meta_th.send_await(())?.map_err(Error::IO)?;
    Ok((meta.len() as usize).div(N))
  }
}
