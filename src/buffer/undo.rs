use std::{
  collections::HashMap,
  ops::Add,
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
  commit_index: usize,
  tx_id: usize,
  data: Page,
  undo_index: usize,
}

impl UndoLog {
  fn new(
    index: usize,
    commit_index: usize,
    tx_id: usize,
    data: Page,
    undo_index: usize,
  ) -> Self {
    Self {
      index,
      commit_index,
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
      self.commit_index,
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
      value.commit_index,
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
    wt.write(&self.commit_index.to_be_bytes())?;
    wt.write(&self.tx_id.to_be_bytes())?;
    wt.write(&self.undo_index.to_be_bytes())?;
    wt.write(self.data.as_ref())?;

    Ok(page)
  }
  fn deserialize(value: &Page<UNDO_PAGE_SIZE>) -> core::result::Result<Self, Error> {
    let mut sc = value.scanner();
    let index = sc.read_usize()?;
    let commit_index = sc.read_usize()?;
    let tx_id = sc.read_usize()?;
    let undo_index = sc.read_usize()?;
    let data = sc.read_n(PAGE_SIZE)?.into();

    Ok(UndoLog::new(index, commit_index, tx_id, data, undo_index))
  }
}

pub struct RollbackStorageConfig {
  fsync_delay: Duration,
  fsync_count: usize,
  max_cache_size: usize,
  max_file_size: usize,
  path: PathBuf,
}

pub struct RollbackStorage {
  cache: Arc<Mutex<LRUCache<usize, UndoLog>>>,
  disk: Arc<PageSeeker<UNDO_PAGE_SIZE>>,
  background: Arc<ThreadPool<Result<()>>>,
  io_c: StoppableChannel<DataBlock, usize>,
  config: RollbackStorageConfig,
}
impl RollbackStorage {
  pub fn open(
    config: RollbackStorageConfig,
    background: Arc<ThreadPool<Result<()>>>,
  ) -> Result<Self> {
    let disk = Arc::new(PageSeeker::open(config.path.as_path())?);
    let cache = Default::default();
    let (io_c, rx) = StoppableChannel::new();

    let storage = Self {
      cache,
      disk,
      background,
      io_c,
      config,
    };
    let cursor = storage.replay()?;
    storage.start_io(rx, cursor);
    Ok(storage)
  }

  fn replay(&self) -> Result<usize> {
    let mut cursor = 0;
    for index in 0..self.config.max_file_size {
      let log: UndoLog = match self.disk.read(index) {
        Ok(page) => match page.deserialize() {
          Ok(log) => log,
          Err(_) => continue,
        },
        Err(_) => break,
      };
      let before = cursor;
      cursor = log.index.add(1).rem_euclid(usize::MAX);
      if index == 0 {
        continue;
      }
      if before != log.index {
        break;
      }
    }

    Ok(cursor)
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
          disk.write(cursor.rem_euclid(max), log.serialize()?)?;
          cursor = cursor.add(1).rem_euclid(usize::MAX);

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

  pub fn get(&self, commit_index: usize, undo_index: usize) -> Result<Page> {
    let mut current = undo_index;
    loop {
      let mut cache = self.cache.l();
      if let Some(log) = cache.get(&current) {
        if commit_index <= log.commit_index {
          return Ok(log.data.copy());
        }
        current = log.undo_index;
        continue;
      }

      let log: UndoLog = self
        .disk
        .read(undo_index.rem_euclid(self.config.max_file_size))?
        .deserialize()?;
      if log.index != undo_index {
        return Err(Error::NotFound);
      }

      cache.insert(undo_index, log.clone());
      if cache.len() >= self.config.max_cache_size {
        cache.pop_old();
      }
      if commit_index <= log.commit_index {
        return Ok(log.data);
      }

      current = log.commit_index
    }
  }

  pub fn append(&self, data: DataBlock) -> Result<usize> {
    Ok(self.io_c.send_with_done(data).recv().unwrap())
  }

  pub fn commit(&self) {
    let mut cache = self.cache.l();
  }
}
