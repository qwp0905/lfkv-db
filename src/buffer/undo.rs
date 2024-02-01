use std::{
  collections::HashMap,
  path::PathBuf,
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use crate::{
  disk::PageSeeker, ContextReceiver, Error, Page, Result, Serializable, ShortenedMutex,
  StoppableChannel, ThreadPool, PAGE_SIZE,
};

use super::{DataBlock, LRUCache};

const UNDO_PAGE_SIZE: usize = PAGE_SIZE + 32;

pub struct UndoLog {
  index: usize,
  log_index: usize,
  tx_id: usize,
  data: Page,
  undo_index: usize,
}

impl UndoLog {
  fn new(
    index: usize,
    log_index: usize,
    tx_id: usize,
    data: Page,
    undo_index: usize,
  ) -> Self {
    Self {
      index,
      log_index,
      tx_id,
      data,
      undo_index,
    }
  }
}
impl Clone for UndoLog {
  fn clone(&self) -> Self {
    Self::new(
      self.index,
      self.log_index,
      self.tx_id,
      self.data.copy(),
      self.undo_index,
    )
  }
}
impl From<DataBlock> for UndoLog {
  fn from(value: DataBlock) -> Self {
    Self::new(
      0,
      value.log_index,
      value.tx_id,
      value.data,
      value.undo_index,
    )
  }
}

impl Serializable<Error, UNDO_PAGE_SIZE> for UndoLog {
  fn serialize(&self) -> core::result::Result<Page<UNDO_PAGE_SIZE>, Error> {
    let mut page = Page::new();
    let mut wt = page.writer();
    wt.write(&self.index.to_be_bytes())?;
    wt.write(&self.log_index.to_be_bytes())?;
    wt.write(&self.tx_id.to_be_bytes())?;
    wt.write(&self.undo_index.to_be_bytes())?;
    wt.write(self.data.as_ref())?;

    Ok(page)
  }
  fn deserialize(value: &Page<UNDO_PAGE_SIZE>) -> core::result::Result<Self, Error> {
    let mut sc = value.scanner();
    let index = sc.read_usize()?;
    let log_index = sc.read_usize()?;
    let tx_id = sc.read_usize()?;
    let undo_index = sc.read_usize()?;
    let data = sc.read_n(PAGE_SIZE)?.into();

    Ok(UndoLog::new(index, log_index, tx_id, data, undo_index))
  }
}

pub struct RollbackStorageConfig {
  fsync_delay: Duration,
  fsync_count: usize,
  max_cache_size: usize,
  max_file_size: usize,
  io_thread_size: usize,
  path: PathBuf,
}

pub struct RollbackStorage {
  cache: Arc<Mutex<LRUCache<usize, UndoLog>>>,
  disk: Arc<PageSeeker<UNDO_PAGE_SIZE>>,
  background: ThreadPool<Result<()>>,
  io_c: StoppableChannel<DataBlock, usize>,
  config: RollbackStorageConfig,
}
impl RollbackStorage {
  pub fn open(config: RollbackStorageConfig) -> Result<()> {
    // let disk = PageSeeker::open(config.path)?;
    // let cache = LRUCache::new();

    Ok(())
  }

  fn start_io(&self, rx: ContextReceiver<DataBlock, usize>, mut cursor: usize) {
    let count = self.config.fsync_count;
    let delay = self.config.fsync_delay;
    let max = self.config.max_file_size;
    let disk = self.disk.clone();

    self.background.schedule(move || {
      let mut start = Instant::now();
      let mut point = delay;
      let mut m = HashMap::new();
      while let Ok(o) = rx.recv_done_or_timeout(delay) {
        if let Some((data, done)) = o {
          m.insert(cursor, done);
          let mut log: UndoLog = data.into();
          log.index = cursor;
          disk.write(cursor % max, log.serialize()?)?;
          cursor = (cursor + 1) % usize::MAX;

          if m.len() < count {
            point -= Instant::now().duration_since(start).min(point);
            start = Instant::now();
            continue;
          }
        }

        if m.len() != 0 {
          disk.fsync()?;
          m.drain().for_each(|(i, done)| done.send(i).unwrap())
        }

        point = delay;
        start = Instant::now();
      }
      Ok(())
    });
  }

  pub fn get(&self, log_index: usize, undo_index: usize) -> Result<Page> {
    if let Some(log) = self.cache.l().get(&undo_index) {
      if log_index <= log.log_index {
        return Ok(log.data.copy());
      }

      return self.get(log_index, log.undo_index);
    }

    let log: UndoLog = self
      .disk
      .read(undo_index % self.config.max_file_size)?
      .deserialize()?;
    if log.index != undo_index {
      return Err(Error::NotFound);
    }

    self.cache.l().insert(undo_index, log.clone());
    if log_index <= log.log_index {
      return Ok(log.data);
    }

    return self.get(log_index, log.undo_index);
  }

  pub fn append(&mut self, data: DataBlock) -> Result<usize> {
    Ok(self.io_c.send_with_done(data).recv().unwrap())
  }
}
