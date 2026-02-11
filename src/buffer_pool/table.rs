use std::{
  hash::{BuildHasher, RandomState},
  sync::{Mutex, MutexGuard},
};

use crate::{buffer_pool::lru::LRUShard, utils::ShortenedMutex};

struct Shard {
  lru: LRUShard<usize, usize>,
  offset: usize,
  reverse: Vec<usize>,
}

pub struct EvictionGuard<'a> {
  frame_id: usize,
  evicted: Option<usize>,
  _guard: MutexGuard<'a, Shard>,
}
impl<'a> EvictionGuard<'a> {
  fn new(frame_id: usize, evicted: Option<usize>, _guard: MutexGuard<'a, Shard>) -> Self {
    Self {
      frame_id,
      evicted,
      _guard,
    }
  }

  pub fn get_frame_id(&self) -> usize {
    self.frame_id
  }
  pub fn get_evicted_index(&self) -> Option<usize> {
    self.evicted
  }
}

pub struct LRUTable {
  shards: Vec<Mutex<Shard>>,
  hasher: RandomState,
  capacity: usize,
}
impl LRUTable {
  pub fn new(shard_count: usize, capacity: usize) -> Self {
    let per = capacity / shard_count;
    let mut shards = Vec::with_capacity(shard_count);
    for i in 0..shard_count {
      let s = Shard {
        reverse: vec![0; per],
        offset: i * per,
        lru: LRUShard::new(per),
      };
      shards.push(Mutex::new(s))
    }

    Self {
      shards,
      hasher: Default::default(),
      capacity: per,
    }
  }
  fn get_shard(&self, key: usize) -> (u64, &Mutex<Shard>) {
    let h = self.hasher.hash_one(key);
    let shard = &self.shards[h as usize % self.shards.len()];
    (h, shard)
  }

  pub fn acquire(&self, index: usize) -> std::result::Result<usize, EvictionGuard<'_>> {
    let (h, s) = self.get_shard(index);
    let mut shard = s.l();
    if let Some(&id) = shard.lru.get(&index, h, &self.hasher) {
      return Ok(id);
    };

    let offset = shard.offset;

    if !shard.lru.is_full() {
      let i = shard.lru.len();
      shard.reverse[i] = index;
      let id = i + offset;
      shard.lru.insert(index, id, h, &self.hasher);
      return Err(EvictionGuard::new(id, None, shard));
    };

    let (evicted, frame_id) = shard.lru.evict(&self.hasher).unwrap();
    shard.lru.insert(index, frame_id, h, &self.hasher);
    shard.reverse[frame_id - offset] = index;
    Err(EvictionGuard::new(frame_id, Some(evicted), shard))
  }
  pub fn get_index(&self, frame_id: usize) -> usize {
    let shard = &self.shards[frame_id / self.capacity].l();
    shard.reverse[frame_id - shard.offset]
  }
}
