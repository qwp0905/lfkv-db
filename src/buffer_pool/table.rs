use std::{
  hash::{BuildHasher, RandomState},
  sync::{Mutex, MutexGuard},
};

use crossbeam::utils::Backoff;

use super::LRUShard;
use crate::utils::ShortenedMutex;

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
    let cap_per_shard = capacity / shard_count;
    let mut shards = Vec::with_capacity(shard_count);
    for i in 0..shard_count {
      let shard = Shard {
        reverse: vec![0; cap_per_shard],
        offset: i * cap_per_shard,
        lru: LRUShard::new(cap_per_shard),
      };
      shards.push(Mutex::new(shard))
    }

    Self {
      shards,
      hasher: Default::default(),
      capacity: cap_per_shard,
    }
  }
  fn get_shard(&self, key: usize) -> (u64, &Mutex<Shard>) {
    let h = self.hasher.hash_one(key);
    let shard = &self.shards[h as usize % self.shards.len()];
    (h, shard)
  }

  pub fn acquire<T>(
    &self,
    index: usize,
    guard_fn: T,
  ) -> std::result::Result<EvictionGuard<'_>, EvictionGuard<'_>>
  where
    T: Fn(usize) -> bool,
  {
    let backoff = Backoff::new();
    let (h, s) = self.get_shard(index);
    let mut shard = s.l();
    if let Some(&id) = shard.lru.get(&index, h, &self.hasher) {
      return Ok(EvictionGuard::new(id, None, shard));
    }

    let offset = shard.offset;
    if !shard.lru.is_full() {
      let i = shard.lru.len();
      shard.reverse[i] = index;
      let id = i + offset;
      shard.lru.insert(index, id, h, &self.hasher);
      return Err(EvictionGuard::new(id, None, shard));
    };

    loop {
      let ((evicted, id), evicted_hash) = shard.lru.evict(&self.hasher).unwrap();
      if !guard_fn(id) {
        shard.lru.insert(evicted, id, evicted_hash, &self.hasher);
        backoff.snooze();
        continue;
      }

      shard.lru.insert(index, id, h, &self.hasher);
      shard.reverse[id - offset] = index;
      return Err(EvictionGuard::new(id, Some(evicted), shard));
    }
  }

  pub fn get_index(&self, frame_id: usize) -> usize {
    let shard = &self.shards[frame_id / self.capacity].l();
    shard.reverse[frame_id - shard.offset]
  }
}

#[cfg(test)]
#[path = "tests/table.rs"]
mod tests;
