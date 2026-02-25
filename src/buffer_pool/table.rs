use std::{
  hash::{BuildHasher, RandomState},
  sync::{Mutex, MutexGuard},
};

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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_cache_miss_then_hit() {
    let table = LRUTable::new(1, 4);

    // first acquire: cache miss
    let guard = table.acquire(42).unwrap_err();
    let frame_id = guard.get_frame_id();
    assert!(guard.get_evicted_index().is_none());
    drop(guard);

    // second acquire: cache hit, same frame_id
    match table.acquire(42) {
      Ok(id) => assert_eq!(id, frame_id),
      Err(_) => panic!("expected cache hit"),
    };
  }

  #[test]
  fn test_multiple_misses_no_eviction() {
    let cap = 4;
    let table = LRUTable::new(1, cap);

    let mut frame_ids = Vec::new();
    for i in 0..cap {
      let guard = table.acquire(i).unwrap_err();
      assert!(guard.get_evicted_index().is_none());
      frame_ids.push(guard.get_frame_id());
      drop(guard);
    }

    // all frame_ids should be unique
    frame_ids.sort();
    frame_ids.dedup();
    assert_eq!(frame_ids.len(), cap);
  }

  #[test]
  fn test_eviction_when_full() {
    let cap = 4;
    let table = LRUTable::new(1, cap);

    // fill capacity
    for i in 0..cap {
      let guard = table.acquire(i).unwrap_err();
      assert!(guard.get_evicted_index().is_none());
      drop(guard);
    }

    // next acquire should trigger eviction
    let guard = table.acquire(100).unwrap_err();
    let evicted = guard.get_evicted_index();
    assert!(evicted.is_some());
    // evicted index should be one of the original entries
    assert!(evicted.unwrap() < cap);
    drop(guard);
  }

  #[test]
  fn test_get_index_reverse_mapping() {
    let cap = 4;
    let table = LRUTable::new(1, cap);

    for i in 0..cap {
      let guard = table.acquire(i * 10).unwrap_err();
      let frame_id = guard.get_frame_id();
      drop(guard);

      assert_eq!(table.get_index(frame_id), i * 10);
    }
  }

  #[test]
  fn test_sharded_cache_hit() {
    // large capacity to avoid eviction from uneven hash distribution
    let table = LRUTable::new(4, 80); // 20 per shard

    let mut entries = Vec::new();
    for i in 0..16 {
      let guard = table.acquire(i).unwrap_err();
      entries.push((i, guard.get_frame_id()));
      drop(guard);
    }

    // all should be cache hits with correct frame_ids
    for (index, expected_frame_id) in &entries {
      match table.acquire(*index) {
        Ok(id) => assert_eq!(id, *expected_frame_id),
        Err(_) => panic!("expected cache hit for index {}", index),
      };
    }
  }

  #[test]
  fn test_sharded_frame_id_ranges() {
    let table = LRUTable::new(4, 80);

    let mut frame_ids = Vec::new();
    for i in 0..16 {
      let guard = table.acquire(i).unwrap_err();
      frame_ids.push(guard.get_frame_id());
      drop(guard);
    }

    // all frame_ids unique
    let mut sorted = frame_ids.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), frame_ids.len());

    // all frame_ids within total capacity range [0, 80)
    for id in &frame_ids {
      assert!(*id < 80, "frame_id {} out of range", id);
    }
  }

  #[test]
  fn test_sharded_get_index_reverse_mapping() {
    let table = LRUTable::new(4, 80);

    let mut pairs = Vec::new();
    for i in 0..16 {
      let guard = table.acquire(i).unwrap_err();
      pairs.push((i, guard.get_frame_id()));
      drop(guard);
    }

    for (index, frame_id) in &pairs {
      assert_eq!(table.get_index(*frame_id), *index);
    }
  }

  #[test]
  fn test_sharded_eviction() {
    // small capacity so eviction happens
    let table = LRUTable::new(4, 8); // 2 per shard

    // insert enough keys to guarantee at least one shard overflows
    let mut inserted = Vec::new();
    let mut eviction_happened = false;
    for i in 0..20 {
      match table.acquire(i) {
        Ok(_) => {} // cache hit from previous insert (shouldn't happen with unique keys)
        Err(guard) => {
          if guard.get_evicted_index().is_some() {
            eviction_happened = true;
          }
          inserted.push((i, guard.get_frame_id()));
          drop(guard);
        }
      };
    }

    assert!(eviction_happened, "expected at least one eviction");

    // frame_ids should all be within [0, 8)
    for (_, frame_id) in &inserted {
      assert!(*frame_id < 8, "frame_id {} out of range", frame_id);
    }
  }

  #[test]
  fn test_eviction_reuses_frame_id() {
    let cap = 4;
    let table = LRUTable::new(1, cap);

    // fill capacity
    for i in 0..cap {
      let guard = table.acquire(i).unwrap_err();
      drop(guard);
    }

    // evict and insert new
    let guard = table.acquire(100).unwrap_err();
    let new_frame_id = guard.get_frame_id();
    drop(guard);

    // frame_id should be within original range (reused)
    assert!(new_frame_id < cap);

    // reverse mapping should point to new index
    assert_eq!(table.get_index(new_frame_id), 100);
  }
}
