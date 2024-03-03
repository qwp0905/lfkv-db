use std::{
  collections::BTreeMap,
  ops::Mul,
  sync::{Arc, Mutex},
};

use crate::{
  disk::Finder, wal::CommitInfo, ContextReceiver, Error, Page, Result, ShortenedMutex,
  StoppableChannel,
};

use super::{CacheStorage, DataBlock, RollbackStorage, BLOCK_SIZE};

pub struct BufferPool {
  cache: Arc<CacheStorage>,
  rollback: Arc<RollbackStorage>,
  uncommitted: Arc<Mutex<BTreeMap<usize, Vec<usize>>>>,
  disk: Arc<Finder<BLOCK_SIZE>>,
  max_cache_size: usize,
}
impl BufferPool {
  pub fn generate(
    rollback: Arc<RollbackStorage>,
    disk: Arc<Finder<BLOCK_SIZE>>,
    max_cache_size: usize,
  ) -> (
    Self,
    StoppableChannel<(), Option<usize>>,
    StoppableChannel<CommitInfo, Result>,
  ) {
    let (write_c, write_rx) = StoppableChannel::new();
    let (flush_c, flush_rx) = StoppableChannel::new();
    let (commit_c, commit_rx) = StoppableChannel::new();
    let bp = Self {
      cache: Arc::new(CacheStorage::new(
        max_cache_size.div_ceil(BLOCK_SIZE),
        write_c,
      )),
      rollback,
      uncommitted: Default::default(),
      disk,
      max_cache_size,
    };

    (
      bp.start_write(write_rx)
        .start_flush(flush_rx)
        .start_commit(commit_rx),
      flush_c,
      commit_c,
    )
  }

  fn start_write(self, rx: ContextReceiver<(usize, Page<BLOCK_SIZE>), Result>) -> Self {
    let disk = self.disk.clone();
    rx.to_done(
      "bufferpool write",
      BLOCK_SIZE.mul(10),
      move |(index, page)| disk.write(index, page),
    );
    self
  }

  fn start_flush(self, rx: ContextReceiver<(), Option<usize>>) -> Self {
    let cache = self.cache.clone();
    let disk = self.disk.clone();
    rx.to_all(
      "bufferpool flush",
      self.max_cache_size.div_ceil(3),
      move |_| {
        let max_index = match cache.flush_all() {
          Ok(o) => o,
          Err(_) => return None,
        };
        if let Err(_) = disk.fsync() {
          return None;
        }

        max_index
      },
    );
    self
  }

  fn start_commit(self, rx: ContextReceiver<CommitInfo, Result>) -> Self {
    let uncommitted = self.uncommitted.clone();
    let cache = self.cache.clone();
    let disk = self.disk.clone();
    let rollback = self.rollback.clone();

    rx.to_new("bufferpool commit", BLOCK_SIZE.mul(10), move |commit| {
      let mut u = uncommitted.l();
      if let Some(v) = u.remove(&commit.tx_id) {
        for index in v {
          let undo_index = match cache.commit(index, commit.as_ref()) {
            Ok(applied) => {
              if applied {
                continue;
              }

              let mut block: DataBlock = disk.read_to(index)?;
              if block.tx_id.eq(&commit.tx_id) {
                block.commit_index = commit.commit_index;
                cache.insert(index, block);
                continue;
              }

              let undo_index = block.undo_index;
              cache.insert(index, block);
              undo_index
            }
            Err(undo_index) => undo_index,
          };
          if let Some(i) = undo_index {
            rollback.commit(i, commit.as_ref())?;
          }
        }
      };
      Ok(())
    });
    self
  }

  // fn start_rollback(&self, rx: ContextReceiver<usize>) {
  //   let uncommitted = self.uncommitted.clone();
  //   let cache = self.cache.clone();

  //   rx.to_done("bufferpool rollback", 1, move |tx_id| {
  //     if let Some(v) = uncommitted.l().get(&tx_id) {
  //       for i in v {}
  //     }
  //   });
  // }

  pub fn get(&self, commit_index: usize, index: usize) -> Result<Page> {
    let block = {
      match self.cache.get(&index) {
        Some(block) => block.copy(),
        None => {
          let block: DataBlock = self.disk.read_to(index)?;
          self.cache.insert(index, block.copy());
          block
        }
      }
    };

    if block.commit_index.le(&commit_index) {
      return Ok(block.data.copy());
    }
    match block.undo_index {
      Some(i) => self.rollback.get(commit_index, i),
      None => Err(Error::NotFound),
    }
  }

  pub fn insert(&self, tx_id: usize, index: usize, data: Page) -> Result<()> {
    let undo_index = {
      match self.cache.get(&index) {
        Some(block) => Some(self.rollback.append(block.copy())?),
        None => match self.disk.read_to::<DataBlock>(index) {
          Ok(block) => Some(self.rollback.append(block)?),
          Err(Error::NotFound) => None,
          Err(err) => return Err(err),
        },
      }
    };

    let new_block = DataBlock::uncommitted(tx_id, undo_index, data);
    self.cache.insert_new(index, new_block);
    self.uncommitted.l().entry(tx_id).or_default().push(index);
    Ok(())
  }
}
