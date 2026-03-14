use std::{
  collections::BTreeSet,
  hash::{BuildHasher, RandomState},
  sync::{Arc, Mutex},
};

use crossbeam::{atomic::AtomicCell, utils::Backoff};

use super::LRUShard;
use crate::utils::{ShortenedMutex, ToArc};

#[derive(Clone, Copy)]
enum Pin {
  Fetched(usize),
  Eviction,
}
impl PartialEq for Pin {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (Pin::Fetched(i), Pin::Fetched(j)) => i == j,
      (Pin::Eviction, Pin::Eviction) => true,
      _ => false,
    }
  }
}
impl Eq for Pin {}

pub struct FrameState {
  pin: AtomicCell<Pin>,
  frame_id: usize,
}
impl FrameState {
  fn try_evict(&self) -> bool {
    self
      .pin
      .compare_exchange(Pin::Fetched(0), Pin::Eviction)
      .is_ok()
  }
  fn try_pin(&self) -> bool {
    let backoff = Backoff::new();
    loop {
      match self.pin.load() {
        Pin::Eviction => return false,
        Pin::Fetched(i) => {
          if self
            .pin
            .compare_exchange(Pin::Fetched(i), Pin::Fetched(i + 1))
            .is_ok()
          {
            return true;
          }

          backoff.spin()
        }
      }
    }
  }

  pub fn get_frame_id(&self) -> usize {
    self.frame_id
  }
  pub fn unpin(&self) {
    let backoff = Backoff::new();
    loop {
      match self.pin.load() {
        Pin::Fetched(i) => {
          if self
            .pin
            .compare_exchange(Pin::Fetched(i), Pin::Fetched(i - 1))
            .is_ok()
          {
            return;
          }
        }
        _ => {}
      }

      backoff.spin()
    }
  }
}

struct Shard {
  lru: LRUShard<usize, Arc<FrameState>>,
  eviction: BTreeSet<usize>, // evicting indexes
}

pub struct EvictionGuard<'a> {
  evicted: Option<(usize, u64)>,
  state: Arc<FrameState>,
  guard: &'a Mutex<Shard>,
  hasher: &'a RandomState,
  new_index: usize,
  new_index_hash: u64,
  committed: bool,
}

impl<'a> EvictionGuard<'a> {
  fn new(
    evicted: Option<(usize, u64)>,
    state: Arc<FrameState>,
    guard: &'a Mutex<Shard>,
    hasher: &'a RandomState,
    new_index: usize,
    new_index_hash: u64,
  ) -> Self {
    Self {
      evicted,
      state,
      guard,
      hasher,
      new_index,
      new_index_hash,
      committed: false,
    }
  }

  pub fn get_frame_id(&self) -> usize {
    self.state.frame_id
  }
  pub fn get_state(&self) -> Arc<FrameState> {
    self.state.clone()
  }
  pub fn get_evicted_index(&self) -> Option<usize> {
    self.evicted.as_ref().map(|(i, _)| *i)
  }
  pub fn commit(&mut self) {
    self.committed = true;
  }
}
impl<'a> Drop for EvictionGuard<'a> {
  fn drop(&mut self) {
    if self.committed {
      if let Some((i, _)) = self.evicted {
        let mut shard = self.guard.l();
        shard.eviction.remove(&i);
      }
      self.state.pin.store(Pin::Fetched(1));
      return;
    }

    // rollback
    let mut shard = self.guard.l();
    if let Some((i, h)) = self.evicted {
      shard.eviction.remove(&i);
      shard.lru.insert(i, self.state.clone(), h, self.hasher);
    }
    shard
      .lru
      .remove(&self.new_index, self.new_index_hash, self.hasher);
    self.state.pin.store(Pin::Fetched(0));
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
      let shard = Shard {
        lru: LRUShard::new(cap_per_shard),
        eviction: BTreeSet::new(),
      };
      shards.push(Mutex::new(shard));
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

  pub fn acquire<'a>(
    &'a self,
    index: usize,
  ) -> std::result::Result<Arc<FrameState>, EvictionGuard<'a>> {
    let (hash, s, offset) = self.get_shard(index);
    let hasher = &self.hasher;
    let backoff = Backoff::new();

    loop {
      let mut shard = s.l();
      if shard.eviction.contains(&index) {
        drop(shard);
        backoff.snooze();
        continue;
      }

      if let Some(state) = shard.lru.get(&index, hash, hasher) {
        if state.try_pin() {
          return Ok(state.clone());
        }

        drop(shard);
        backoff.snooze();
        continue;
      }

      if !shard.lru.is_full() {
        let frame_id = shard.lru.len() + offset;
        let state = FrameState {
          frame_id,
          pin: AtomicCell::new(Pin::Eviction),
        }
        .to_arc();
        shard.lru.insert(index, state.clone(), hash, hasher);
        return Err(EvictionGuard::new(None, state, &s, hasher, index, hash));
      }

      let ((evicted, state), evicted_hash) = shard.lru.evict(&self.hasher).unwrap();
      if !state.try_evict() {
        shard.lru.insert(evicted, state, evicted_hash, hasher);
        drop(shard);
        backoff.snooze();
        continue;
      }

      shard.eviction.insert(evicted);
      shard.lru.insert(index, state.clone(), hash, hasher);
      return Err(EvictionGuard::new(
        Some((evicted, evicted_hash)),
        state,
        &s,
        hasher,
        index,
        hash,
      ));
    }
  }
}

#[cfg(test)]
#[path = "tests/table.rs"]
mod tests;
