use std::{
  collections::BTreeMap,
  ops::Mul,
  sync::{Arc, Mutex},
};

use crate::{
  disk::Finder, wal::CommitInfo, BackgroundThread, BackgroundWork, Error, Page, Result,
  ShortenedMutex,
};

use super::{CacheStorage, DataBlock, RollbackStorage, BLOCK_SIZE};

pub struct BufferPool {
  cache: Arc<CacheStorage>,
  rollback: Arc<RollbackStorage>,
  uncommitted: Arc<Mutex<BTreeMap<usize, Vec<usize>>>>,
  disk: Arc<Finder<BLOCK_SIZE>>,
}
impl BufferPool {
  pub fn generate(
    rollback: Arc<RollbackStorage>,
    disk: Arc<Finder<BLOCK_SIZE>>,
    max_cache_size: usize,
  ) -> (
    Self,
    BackgroundThread<(), Option<usize>>,
    BackgroundThread<CommitInfo, Result>,
  ) {
    let disk_cloned = disk.clone();
    let write_c = BackgroundThread::new(
      "bufferpool write",
      max_cache_size.div_ceil(3),
      BackgroundWork::no_timeout(move |(index, page)| {
        disk_cloned.batch_write(index, page)
      }),
    );

    let cache = Arc::new(CacheStorage::new(
      max_cache_size.div_ceil(BLOCK_SIZE),
      write_c,
    ));

    let uncommitted: Arc<Mutex<BTreeMap<usize, Vec<usize>>>> = Default::default();

    let disk_cloned = disk.clone();
    let cache_cloned = cache.clone();
    let flush_c = BackgroundThread::new(
      "bufferpool flush",
      max_cache_size.div_ceil(3),
      BackgroundWork::no_timeout(move |_| {
        let max_index = match cache_cloned.flush_all() {
          Ok(o) => o,
          Err(_) => return None,
        };
        if let Err(_) = disk_cloned.fsync() {
          return None;
        }

        max_index
      }),
    );

    let uncommitted_cloned = uncommitted.clone();
    let disk_cloned = disk.clone();
    let cache_cloned = cache.clone();
    let rollback_cloned = rollback.clone();
    let commit_c = BackgroundThread::new(
      "bufferpool commit",
      BLOCK_SIZE.mul(100),
      BackgroundWork::no_timeout(move |commit: CommitInfo| {
        let mut u = uncommitted_cloned.l();
        if let Some(v) = u.remove(&commit.tx_id) {
          for index in v {
            let undo_index = match cache_cloned.commit(index, commit.as_ref()) {
              Ok(applied) => {
                if applied {
                  continue;
                }

                let mut block: DataBlock = disk_cloned.read_to(index)?;
                if block.tx_id.eq(&commit.tx_id) {
                  block.commit_index = commit.commit_index;
                  cache_cloned.insert(index, block);
                  continue;
                }

                let undo_index = block.undo_index;
                cache_cloned.insert(index, block);
                undo_index
              }
              Err(undo_index) => undo_index,
            };
            if let Some(i) = undo_index {
              rollback_cloned.commit(i, commit.as_ref())?;
            }
          }
        };
        Ok(())
      }),
    );

    (
      Self {
        cache,
        rollback,
        uncommitted,
        disk,
      },
      flush_c,
      commit_c,
    )
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

  pub fn before_shutdown(&self) {
    self.cache.before_shutdown();
    self.rollback.destroy();
  }
}
