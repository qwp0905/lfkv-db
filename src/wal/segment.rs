use std::{
  fs::{remove_file, File, OpenOptions},
  io::IoSlice,
  mem::transmute,
  path::{Path, PathBuf},
  sync::{Arc, Mutex},
  time::Duration,
};

use super::WAL_BLOCK_SIZE;
use crate::{
  constant::FILE_SUFFIX,
  disk::{DirectIO, Page, Pread, Pwritev},
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

const IO_BUFFER_COUNT: usize = 30;
const IO_BUFFER_TIMEOUT: Duration = Duration::from_micros(100);

pub struct WALSegment {
  file: Arc<File>,
  path: Mutex<PathBuf>,
  flush: SingleWorkThread<(), bool>,
  io: SingleWorkThread<(usize, &'static [u8]), Result>,
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
    file.sync_all().map_err(Error::IO)?;
    Ok(Self::new(file, path.into(), flush_count, flush_interval))
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
    Ok(Self::new(
      file,
      path.as_ref().into(),
      flush_count,
      flush_interval,
    ))
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
      .io
      .send((index, unsafe { transmute(page.as_ref().as_ref()) }))
      .wait_flatten()
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

  fn new(
    file: Arc<File>,
    path: PathBuf,
    flush_count: usize,
    flush_interval: Duration,
  ) -> Self {
    let io = WorkBuilder::new()
      .name(format!(
        "{} buffered write",
        path.as_path().to_string_lossy()
      ))
      .stack_size(2 << 20)
      .single()
      .buffering(
        IO_BUFFER_TIMEOUT,
        IO_BUFFER_COUNT,
        handle_write_result,
        handle_write(file.clone()),
      );

    let flush = WorkBuilder::new()
      .name(format!("{} flush", path.as_path().to_string_lossy()))
      .stack_size(2 << 20)
      .single()
      .buffering(
        flush_interval,
        flush_count,
        |(_, r)| r,
        handle_flush(file.clone()),
      );
    Self {
      file,
      io,
      flush,
      path: Mutex::new(path),
    }
  }

  pub fn fsync(&self) -> FsyncResult {
    FsyncResult(self.flush.send(()))
  }

  pub fn truncate(self) -> Result {
    self.io.close();
    self.flush.close();
    remove_file(self.path.l().as_path()).map_err(Error::IO)?;
    Ok(())
  }

  pub fn close(&self) {
    self.flush.close();
  }
}

fn handle_flush(file: Arc<File>) -> impl Fn(&Vec<()>) -> bool {
  move |_| file.sync_data().is_ok()
}

fn pad_start(n: usize) -> String {
  format!("{:0>20}", n)
}
fn handle_write_result((_, result): ((usize, &[u8]), bool)) -> Result {
  result
    .then(|| Ok(()))
    .unwrap_or(Err(Error::BufferedWriteFailed))
}
fn handle_write(file: Arc<File>) -> impl FnMut(&Vec<(usize, &[u8])>) -> bool {
  move |buffered| {
    let mut sorted: Vec<_> = buffered.iter().map(|(i, slice)| (*i, slice)).collect();
    sorted.sort_by_key(|(i, _)| *i);
    sorted.dedup_by_key(|(i, _)| *i);

    for (index, bufs) in sorted.chunk_by(|(a, _), (b, _)| *a + 1 == *b).map(|group| {
      let i = group[0].0;
      let bufs = group
        .into_iter()
        .map(|(_, s)| IoSlice::new(*s))
        .collect::<Vec<_>>();
      (i, bufs)
    }) {
      match file.pwritev(&bufs, (index * WAL_BLOCK_SIZE) as u64) {
        Ok(_) => continue,
        Err(_) => return false,
      }
    }

    true
  }
}
