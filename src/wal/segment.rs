use std::{
  fs::{remove_file, File, OpenOptions},
  path::{Path, PathBuf},
  sync::{Arc, Mutex},
  time::Duration,
};

use super::WAL_BLOCK_SIZE;
use crate::{
  constant::FILE_SUFFIX,
  disk::{DirectIO, Page, Pread, Pwrite},
  error::Result,
  thread::{SingleWorkThread, WorkBuilder, WorkResult},
  utils::{ShortenedMutex, ToArc},
  Error,
};

pub struct FsyncResult(WorkResult<bool>);
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
  file: Arc<File>,
  path: Mutex<PathBuf>,
  flush: SingleWorkThread<(), bool>,
}
impl WALSegment {
  pub fn parse_generation<A, B>(filename: &A, prefix: &B) -> Result<usize>
  where
    A: AsRef<str>,
    B: AsRef<str>,
  {
    let generation: usize = filename
      .as_ref()
      .replace(prefix.as_ref(), "")
      .trim_end_matches(FILE_SUFFIX)
      .parse()
      .map_err(Error::unknown)?;
    Ok(generation)
  }
  pub fn open_exists<P: AsRef<Path>>(
    path: P,
    flush_count: usize,
    flush_interval: Duration,
  ) -> Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .direct_io()
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
      path: Mutex::new(path.as_ref().into()),
    })
  }

  pub fn read<P: AsMut<Page<WAL_BLOCK_SIZE>>>(
    &self,
    index: usize,
    page: &mut P,
  ) -> Result {
    self
      .file
      .pread(page.as_mut().as_mut(), (index * WAL_BLOCK_SIZE) as u64)
      .map(|_| ())
      .map_err(Error::IO)
  }
  pub fn write<P: AsRef<Page<WAL_BLOCK_SIZE>>>(&self, index: usize, page: &P) -> Result {
    self
      .file
      .pwrite(page.as_ref().as_ref(), (index * WAL_BLOCK_SIZE) as u64)
      .map(|_| ())
      .map_err(Error::IO)
  }
  pub fn len(&self) -> Result<usize> {
    let metadata = self.file.metadata().map_err(Error::IO)?;
    Ok((metadata.len() as usize).div_ceil(WAL_BLOCK_SIZE))
  }

  pub fn reuse<P: AsRef<Path>>(&self, prefix: P, generation: usize) -> Result {
    let new_path = format!(
      "{}{}{}",
      prefix.as_ref().to_string_lossy(),
      pad_start(generation),
      FILE_SUFFIX
    );
    let mut path = self.path.l();
    std::fs::rename(path.as_path(), &new_path).map_err(Error::IO)?;
    *path = PathBuf::from(new_path);
    Ok(())
  }

  pub fn open_new<P: AsRef<Path>>(
    prefix: P,
    generation: usize,
    flush_count: usize,
    flush_interval: Duration,
    max_len: usize,
  ) -> Result<Self> {
    let path = format!(
      "{}{}{}",
      prefix.as_ref().to_string_lossy(),
      pad_start(generation),
      FILE_SUFFIX
    );

    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .direct_io()
      .open(&path)
      .map_err(Error::IO)?
      .to_arc();

    file
      .set_len((WAL_BLOCK_SIZE * max_len) as u64)
      .map_err(Error::IO)?;

    let flush = WorkBuilder::new()
      .name(format!("{} flush", &path))
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
      path: Mutex::new(path.into()),
    })
  }

  pub fn fsync(&self) -> FsyncResult {
    FsyncResult(self.flush.send(()))
  }

  pub fn truncate(self) -> Result {
    self.flush.close();
    remove_file(self.path.l().as_path()).map_err(Error::IO)?;
    Ok(())
  }

  pub fn close(&self) {
    self.flush.close();
  }
}

fn handle_flush(file: Arc<File>) -> impl Fn(()) -> bool {
  move |_| file.sync_all().is_ok()
}

fn pad_start(n: usize) -> String {
  format!("{:0>20}", n)
}
