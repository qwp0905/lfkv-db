use std::{
  collections::BTreeMap,
  sync::{Arc, Mutex, RwLock},
};

use crate::{
  disk::PageSeeker, size, wal::CommitInfo, ContextReceiver, EmptySender, Error, Page,
  Result, Serializable, ShortenedMutex, StoppableChannel, ThreadPool, PAGE_SIZE,
};

use super::{CacheStorage, LRUCache, RollbackStorage};

pub const BLOCK_SIZE: usize = size::kb(4);

pub struct DataBlock {
  pub commit_index: usize,
  pub tx_id: usize,
  pub undo_index: usize,
  pub data: Page,
}

impl DataBlock {
  pub fn new(commit_index: usize, tx_id: usize, undo_index: usize, data: Page) -> Self {
    Self {
      commit_index,
      tx_id,
      undo_index,
      data,
    }
  }

  pub fn copy(&self) -> Self {
    Self::new(
      self.commit_index,
      self.tx_id,
      self.undo_index,
      self.data.copy(),
    )
  }
}
impl Serializable<Error, BLOCK_SIZE> for DataBlock {
  fn serialize(&self) -> std::prelude::v1::Result<Page<BLOCK_SIZE>, Error> {
    let mut page = Page::new();
    let mut wt = page.writer();
    wt.write(self.commit_index.to_be_bytes().as_ref())?;
    wt.write(self.tx_id.to_be_bytes().as_ref())?;
    wt.write(self.undo_index.to_be_bytes().as_ref())?;
    wt.write(self.data.as_ref())?;
    Ok(page)
  }
  fn deserialize(value: &Page<BLOCK_SIZE>) -> std::prelude::v1::Result<Self, Error> {
    let mut sc = value.scanner();
    let commit_index = sc.read_usize()?;
    let tx_id = sc.read_usize()?;
    let undo_index = sc.read_usize()?;
    let data = sc.read_n(PAGE_SIZE)?.into();
    Ok(Self::new(commit_index, tx_id, undo_index, data))
  }
}

pub struct BufferPool {
  cache: Arc<CacheStorage>,
  rollback: Arc<RollbackStorage>,
  disk: Arc<PageSeeker<BLOCK_SIZE>>,
  max_cache_size: usize,
}
impl BufferPool {
  pub fn get(&self, commit_index: usize, index: usize) -> Result<Page> {
    let block = {
      match self.cache.get(&index) {
        Some(block) => block.copy(),
        None => {
          let block: DataBlock = self.disk.read(index)?.deserialize()?;
          self.cache.insert(index, block.copy());
          block
        }
      }
    };
    if block.commit_index >= commit_index {
      return Ok(block.data.copy());
    }
    return self.rollback.get(commit_index, block.undo_index);
  }

  pub fn insert(&self) {}
}

// use super::PageCache;

// pub struct BufferPool {
//   cache: Arc<PageCache>,
//   disk: Arc<PageSeeker>,
//   background: Arc<ThreadPool<Result<()>>>,
//   flush_c: StoppableChannel<Vec<(usize, usize, Page)>>,
// }

// impl BufferPool {
//   fn start_flush(&self, rx: ContextReceiver<Vec<(usize, usize, Page)>>) {
//     let cache = self.cache.clone();
//     let disk = self.disk.clone();
//     self.background.schedule(move || {
//       while let Ok((v, done)) = rx.recv_all() {
//         for (tx_id, i, p) in v {
//           disk.write(i, p)?;
//           cache.flush(tx_id, i);
//         }

//         disk.fsync()?;
//         done.map(|tx| tx.close());
//       }
//       Ok(())
//     })
//   }

//   fn start_commit(&self, rx: ContextReceiver<CommitInfo>) {
//     let cache = self.cache.clone();
//     self.background.schedule(move || {
//       while let Ok(commit) = rx.recv_new() {
//         let indexes = match cache.uncommitted(commit.tx_id) {
//           None => continue,
//           Some(indexes) => indexes,
//         };
//         for index in indexes {}
//         // cache.commit(tx_id);
//       }
//       Ok(())
//     });
//   }
// }
