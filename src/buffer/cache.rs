use std::{
  collections::{BTreeMap, BTreeSet},
  sync::Mutex,
};

use crate::{
  wal::CommitInfo, Drain, DroppableReceiver, Page, Result, Serializable, ShortenedMutex,
  StoppableChannel,
};

use super::{DataBlock, LRUCache, BLOCK_SIZE};

pub struct CacheStorage(Mutex<CacheStorageCore>);
struct CacheStorageCore {
  cache: LRUCache<usize, DataBlock>,
  evicted: BTreeMap<usize, DataBlock>,
  max_cache_size: usize,
  dirty: BTreeSet<usize>,
  write_c: StoppableChannel<(usize, Page<BLOCK_SIZE>), Result>,
}
impl CacheStorage {
  pub fn new(
    max_cache_size: usize,
    write_c: StoppableChannel<(usize, Page<BLOCK_SIZE>), Result>,
  ) -> Self {
    Self(Mutex::new(CacheStorageCore {
      cache: Default::default(),
      evicted: Default::default(),
      max_cache_size,
      dirty: Default::default(),
      write_c,
    }))
  }

  pub fn get(&self, index: &usize) -> Option<DataBlock> {
    let mut core = self.0.l();
    if let Some(block) = core.cache.get(index) {
      return Some(block.copy());
    }

    core.evicted.remove(index).and_then(|block| {
      core.cache.insert(*index, block.copy());
      if core.cache.len().ge(&core.max_cache_size) {
        core.cache.pop_old().map(|(i, b)| core.evicted.insert(i, b));
      };
      Some(block)
    })
  }

  pub fn insert(&self, index: usize, block: DataBlock) {
    let mut core = self.0.l();
    core.evicted.remove(&index);
    core.cache.insert(index, block);
    if core.cache.len().ge(&core.max_cache_size) {
      core.cache.pop_old().map(|(i, b)| core.evicted.insert(i, b));
    }
  }

  pub fn insert_new(&self, index: usize, block: DataBlock) {
    let mut core = self.0.l();
    core.dirty.insert(index);
    core.evicted.remove(&index);
    core.cache.insert(index, block);
    if core.cache.len().ge(&core.max_cache_size) {
      core.cache.pop_old().map(|(i, b)| core.evicted.insert(i, b));
    }
  }

  pub fn commit(
    &self,
    index: usize,
    commit: &CommitInfo,
  ) -> core::result::Result<bool, usize> {
    let mut core = self.0.l();
    if let Some(block) = core.cache.get_mut(&index) {
      if block.tx_id.eq(&commit.tx_id) {
        block.commit_index = commit.commit_index;
        return Ok(true);
      }

      return Err(block.undo_index);
    };

    if let Some(block) = core.evicted.get_mut(&index) {
      if block.tx_id.eq(&commit.tx_id) {
        block.commit_index = commit.commit_index;
        return Ok(true);
      }

      return Err(block.undo_index);
    }

    Ok(false)
  }

  pub fn flush_all(&self) -> Result<Option<usize>> {
    let (max_index, wait) = {
      let mut l = vec![];
      let mut core = self.0.l();
      if core.dirty.is_empty() {
        core.evicted.clear();
        return Ok(None);
      }

      let indexes = core.dirty.drain();
      let mut max = 0;
      for i in indexes {
        if let Some(block) = core.cache.get_mut(&i) {
          max = block.commit_index.max(max);
          let page = block.serialize()?;
          l.push(core.write_c.send_with_done((i, page)));
          continue;
        }

        if let Some(block) = core.evicted.remove(&i) {
          max = block.commit_index.max(max);
          let page = block.serialize()?;
          l.push(core.write_c.send_with_done((i, page)));
        }
      }
      core.evicted.clear();
      (max, l)
    };

    for r in wait {
      r.drop_one()
    }

    Ok(Some(max_index))
  }
}
