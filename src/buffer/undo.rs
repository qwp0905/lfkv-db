use std::{ops::Add, path::PathBuf, sync::Mutex, time::Duration};

use crate::{
  disk::{Finder, FinderConfig},
  wal::CommitInfo,
  Error, Page, Result, Serializable, ShortenedMutex, PAGE_SIZE,
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

  fn from_data(index: usize, data: DataBlock) -> Self {
    Self::new(
      index,
      data.commit_index,
      data.tx_id,
      data.data,
      data.undo_index,
    )
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
  cache: Mutex<LRUCache<usize, UndoLog>>,
  disk: Finder<UNDO_PAGE_SIZE>,
  config: RollbackStorageConfig,
  cursor: Mutex<usize>,
}
impl RollbackStorage {
  pub fn open(config: RollbackStorageConfig) -> Result<Self> {
    let disk = Finder::open(FinderConfig {
      path: config.path.clone(),
      batch_delay: config.fsync_delay,
      batch_size: config.fsync_count,
    })?;
    let cache = Default::default();
    let cursor = Default::default();

    let storage = Self {
      cache,
      disk,
      config,
      cursor,
    };
    storage.replay()?;
    Ok(storage)
  }

  fn replay(&self) -> Result<()> {
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
    *self.cursor.l() = cursor;

    Ok(())
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
        .read_to(undo_index.rem_euclid(self.config.max_file_size))?;
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

      current = log.undo_index
    }
  }

  pub fn append(&self, data: DataBlock) -> Result<usize> {
    let index = {
      let mut c = self.cursor.l();
      let index = c.add(1).rem_euclid(usize::MAX);
      *c = index;
      index
    };
    self
      .disk
      .batch_write_from(index, &UndoLog::from_data(index, data))?;
    Ok(index)
  }

  pub fn commit(&self, undo_index: usize, commit: &CommitInfo) -> Result<()> {
    let mut current = undo_index;
    loop {
      let mut cache = self.cache.l();
      if let Some(log) = cache.get_mut(&current) {
        if commit.tx_id == log.tx_id {
          log.commit_index = commit.commit_index;
          return Ok(());
        }
        current = log.undo_index;
        continue;
      }

      let mut log: UndoLog = self
        .disk
        .read_to(undo_index.rem_euclid(self.config.max_file_size))?;
      if log.index != undo_index {
        return Err(Error::NotFound);
      }

      if commit.tx_id == log.tx_id {
        log.commit_index = commit.commit_index;

        cache.insert(undo_index, log);
        if cache.len() >= self.config.max_cache_size {
          cache.pop_old();
        }

        return Ok(());
      }

      current = log.undo_index
    }
  }
}
