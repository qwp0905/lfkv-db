use std::{
  fs::{remove_file, File, OpenOptions},
  path::{Path, PathBuf},
  sync::Arc,
  time::Duration,
};

use chrono::Local;

use super::WAL_BLOCK_SIZE;
use crate::{
  constant::FILE_SUFFIX,
  disk::{PageRef, Pread, Pwrite},
  error::Result,
  thread::{Oneshot, SingleWorkThread, WorkBuilder},
  utils::ToArc,
  Error,
};

pub struct FsyncResult(Oneshot<Result<bool>>);
impl FsyncResult {
  pub fn wait(self) -> Result {
    self
      .0
      .wait_result()?
      .then(|| Ok(()))
      .unwrap_or_else(|| Err(Error::FlushFailed))
  }
}

pub struct WALSegment {
  file: Arc<File>,
  path: PathBuf,
  flush: SingleWorkThread<(), bool>,
}
impl WALSegment {
  pub fn open_exists<P: AsRef<Path>>(
    path: P,
    flush_count: usize,
    flush_interval: Duration,
  ) -> Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(path.as_ref())
      .map_err(Error::IO)?
      .to_arc();

    let flush = WorkBuilder::new()
      .name(format!("{} flush", path.as_ref().to_string_lossy()))
      .stack_size(2 << 20)
      .single()
      .buffering(
        flush_interval,
        flush_count,
        |(_, r)| r,
        handle_flush(file.clone()),
      );
    Ok(Self {
      file,
      flush,
      path: path.as_ref().into(),
    })
  }

  pub fn read(&self, index: usize, page: &mut PageRef<WAL_BLOCK_SIZE>) -> Result {
    self
      .file
      .pread(page.as_mut().as_mut(), (index * WAL_BLOCK_SIZE) as u64)
      .map(|_| ())
      .map_err(Error::IO)
  }
  pub fn write(&self, index: usize, page: &PageRef<WAL_BLOCK_SIZE>) -> Result {
    self
      .file
      .pwrite(page.as_ref().as_ref(), (index * WAL_BLOCK_SIZE) as u64)
      .map(|_| ())
      .map_err(Error::IO)
  }
  pub fn len(&self) -> Result<usize> {
    let metadata = self.file.metadata().map_err(Error::IO)?;
    Ok(metadata.len() as usize / WAL_BLOCK_SIZE)
  }

  pub fn open_new<P: AsRef<Path>>(
    prefix: P,
    flush_count: usize,
    flush_interval: Duration,
  ) -> Result<Self> {
    Self::open_exists(
      format!(
        "{}{}{}",
        prefix.as_ref().to_string_lossy(),
        Local::now().timestamp_millis(),
        FILE_SUFFIX
      ),
      flush_count,
      flush_interval,
    )
  }

  pub fn fsync(&self) -> FsyncResult {
    FsyncResult(self.flush.send(()))
  }

  pub fn unlink(&self) -> Result {
    remove_file(&self.path).map_err(Error::IO)
  }

  pub fn close(&self) {
    self.flush.close();
  }
}

fn handle_flush(file: Arc<File>) -> impl Fn(()) -> bool {
  move |_| file.sync_all().is_ok()
}
