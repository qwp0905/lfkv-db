use std::{
  hash::{BuildHasher, RandomState},
  sync::{Mutex, MutexGuard},
};

use crossbeam::utils::Backoff;

use super::LRUShard;
use crate::utils::ShortenedMutex;

type Shard = LRUShard<usize, usize>;

pub struct EvictionGuard<'a> {
  frame_id: usize,
  succeed: bool,
  evicted: Option<(usize, u64)>,
  new_index: usize,
  new_index_hash: u64,
  hasher: &'a RandomState,
  guard: MutexGuard<'a, Shard>,
}
impl<'a> EvictionGuard<'a> {
  fn new(
    frame_id: usize,
    new_index: usize,
    new_index_hash: u64,
    evicted: Option<(usize, u64)>,
    hasher: &'a RandomState,
    guard: MutexGuard<'a, Shard>,
    succeed: bool,
  ) -> Self {
    Self {
      frame_id,
      new_index,
      new_index_hash,
      evicted,
      hasher,
      guard,
      succeed,
    }
  }

  pub fn get_frame_id(&self) -> usize {
    self.frame_id
  }
  pub fn get_evicted_index(&self) -> Option<usize> {
    self.evicted.as_ref().map(|(i, _)| *i)
  }

  pub fn take(&mut self) -> Option<usize> {
    self.evicted.take().map(|(index, _)| index)
  }
  pub fn is_succeeded(&self) -> bool {
    self.succeed
  }
}
impl<'a> Drop for EvictionGuard<'a> {
  fn drop(&mut self) {
    if self.succeed {
      return;
    }
    let (index, hash) = self
      .evicted
      .take()
      .unwrap_or_else(|| (self.new_index, self.new_index_hash));
    self.guard.insert(index, self.frame_id, hash, self.hasher);
  }
}

pub struct LRUTable {
  shards: Vec<Mutex<Shard>>,
  offset: Vec<usize>,
  hasher: RandomState,
}
impl LRUTable {
  pub fn new(shard_count: usize, capacity: usize) -> Self {
    let cap_per_shard = capacity / shard_count;
    let mut shards = Vec::with_capacity(shard_count);
    let mut offset = Vec::with_capacity(shard_count);
    for i in 0..shard_count {
      shards.push(Mutex::new(LRUShard::new(cap_per_shard)));
      offset.push(i * cap_per_shard);
    }

    Self {
      shards,
      offset,
      hasher: Default::default(),
    }
  }
  fn get_shard(&self, key: usize) -> (u64, &Mutex<Shard>, usize) {
    let h = self.hasher.hash_one(key);
    let i = h as usize % self.shards.len();
    let shard = &self.shards[i];
    let offset = self.offset[i];
    (h, shard, offset)
  }

  pub fn acquire<F>(&self, index: usize, guard_fn: F) -> EvictionGuard<'_>
  where
    F: Fn(usize) -> bool,
  {
    let backoff = Backoff::new();
    let (h, s, offset) = self.get_shard(index);
    loop {
      let mut shard = s.l();
      if let Some(&id) = shard.get(&index, h, &self.hasher) {
        return EvictionGuard::new(id, 0, 0, None, &self.hasher, shard, true);
      }

      if !shard.is_full() {
        let i = shard.len();
        let id = i + offset;
        return EvictionGuard::new(id, index, h, None, &self.hasher, shard, false);
      };

      let ((evicted, id), evicted_hash) = shard.evict(&self.hasher).unwrap();
      if guard_fn(id) {
        return EvictionGuard::new(
          id,
          index,
          h,
          Some((evicted, evicted_hash)),
          &self.hasher,
          shard,
          false,
        );
      }

      shard.insert(evicted, id, evicted_hash, &self.hasher);
      drop(shard);

      backoff.snooze();
    }
  }
}

#[cfg(test)]
#[path = "tests/table.rs"]
mod tests;
