use std::{
  collections::{BTreeMap, HashMap, HashSet},
  sync::Mutex,
};

use crate::{Page, ShortenedMutex};

use super::{DataBlock, LRUCache};

pub struct CacheStorage(Mutex<CacheStorageCore>);
struct CacheStorageCore {
  cache: LRUCache<usize, DataBlock>,
  evicted: BTreeMap<usize, DataBlock>,
  max_cache_size: usize,
}
impl CacheStorage {
  pub fn get(&self, index: usize) -> Option<DataBlock> {
    let mut core = self.0.l();
    core.cache.get(&index).map(|block| block.copy())
  }

  pub fn insert(&self, index: usize, block: DataBlock) {
    let mut core = self.0.l();
    core.cache.insert(index, block);
    if core.cache.len() >= core.max_cache_size {
      core.cache.pop_old().map(|(i, b)| core.evicted.insert(i, b));
    }
  }
}

// pub struct PageCache(Mutex<PageCacheCore>);
// struct PageCacheCore {
//   cache: LRUCache<usize, MVCC>,
//   uncommitted: HashMap<usize, HashSet<usize>>,
//   evicted: HashMap<usize, MVCC>,
//   max_cache_size: usize,
// }
// impl PageCache {
//   pub fn get(&self, tx_id: usize, index: usize) -> Option<Page> {
//     let mut core = self.0.l();
//     if let Some(mvcc) = core.cache.get(&index) {
//       return mvcc.view(tx_id).map(|page| page.copy());
//     }

//     return core
//       .evicted
//       .get(&index)
//       .and_then(|mvcc| mvcc.view(tx_id))
//       .map(|page| page.copy());
//   }

//   pub fn insert_dirty(&self, tx_id: usize, index: usize, page: Page) {
//     let mut core = self.0.l();
//     core.uncommitted.entry(tx_id).or_default().insert(index);
//     core
//       .cache
//       .entry(index)
//       .or_default()
//       .append_uncommitted(tx_id, page);

//     if core.cache.len() < core.max_cache_size {
//       return;
//     }

//     core
//       .cache
//       .pop_old()
//       .map(|(i, mvcc)| core.evicted.insert(i, mvcc));
//   }

//   pub fn insert_from_disk(&self, tx_id: usize, index: usize, page: Page) {
//     let mut core = self.0.l();
//     core
//       .cache
//       .entry(index)
//       .or_default()
//       .append_committed(tx_id, page);

//     if core.cache.len() < core.max_cache_size {
//       return;
//     }

//     core
//       .cache
//       .pop_old()
//       .map(|(i, mvcc)| core.evicted.insert(i, mvcc));
//   }

//   pub fn commit(&self, tx_id: usize) {
//     let mut core = self.0.l();
//     let indexes = match core.uncommitted.remove(&tx_id) {
//       None => return,
//       Some(v) => v,
//     };
//     for index in indexes {
//       core.cache.get_mut(&index).map(|mvcc| mvcc.commit(tx_id));
//       core.evicted.get_mut(&index).map(|mvcc| mvcc.commit(tx_id));
//     }
//   }

//   pub fn flush(&self, tx_id: usize, index: usize) {
//     let mut core = self.0.l();
//     if let Some(mvcc) = core.cache.get_mut(&index) {
//       mvcc.split_off(tx_id);
//     };
//     if let Some(mvcc) = core.evicted.get_mut(&index) {
//       mvcc.split_off(tx_id + 1);
//       if mvcc.is_empty() {
//         core.evicted.remove(&index);
//       }
//     }
//   }

//   pub fn uncommitted(&self, tx_id: usize) -> Option<HashSet<usize>> {
//     self.0.l().uncommitted.remove(&tx_id)
//   }
// }
