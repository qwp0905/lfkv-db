use std::{
  fs::{remove_file, File, OpenOptions},
  path::{Path, PathBuf},
  sync::Arc,
  time::Duration,
};

use chrono::Local;
use crossbeam::queue::ArrayQueue;

use super::WAL_BLOCK_SIZE;
use crate::{
  constant::FILE_SUFFIX,
  disk::{PageRef, Pread, Pwrite},
  error::Result,
  thread::{Oneshot, OneshotFulfill, SingleWorkThread, WorkBuilder},
  utils::ToArc,
  Error,
};

pub struct FsyncResult(Oneshot<Result<bool>>);
impl FsyncResult {
  pub fn wait(self) -> Result {
    self
      .0
      .wait()?
      .then(|| Ok(()))
      .unwrap_or_else(|| Err(Error::FlushFailed))
  }
}

pub struct WALSegment {
  disk: Arc<File>,
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
      .with_timer(flush_interval, handle_flush(flush_count, file.clone()));
    Ok(Self {
      disk: file,
      flush,
      path: path.as_ref().into(),
    })
  }

  pub fn read(&self, index: usize, page: &mut PageRef<WAL_BLOCK_SIZE>) -> Result {
    self
      .disk
      .pread(page.as_mut().as_mut(), (index * WAL_BLOCK_SIZE) as u64)
      .map(|_| ())
      .map_err(Error::IO)
  }
  pub fn write(&self, index: usize, page: &PageRef<WAL_BLOCK_SIZE>) -> Result {
    self
      .disk
      .pwrite(page.as_ref().as_ref(), (index * WAL_BLOCK_SIZE) as u64)
      .map(|_| ())
      .map_err(Error::IO)
  }
  pub fn len(&self) -> Result<usize> {
    let metadata = self.disk.metadata().map_err(Error::IO)?;
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

fn handle_flush(
  count: usize,
  file: Arc<File>,
) -> impl Fn(Option<((), OneshotFulfill<Result<bool>>)>) -> bool {
  let waits = ArrayQueue::new(count);
  move |v: Option<((), OneshotFulfill<Result<bool>>)>| {
    if let Some((_, done)) = v {
      let _ = waits.push(done);
      if !waits.is_full() {
        return false;
      }
    }

    if waits.is_empty() {
      return true;
    }

    let result = file.sync_all().is_ok();
    while let Some(done) = waits.pop() {
      done.fulfill(Ok(result));
    }

    true
  }
}
