use std::{
  collections::{BTreeMap, BTreeSet},
  hash::{BuildHasher, RandomState},
  sync::{Arc, Mutex},
};

use crossbeam::utils::Backoff;

use super::{FrameState, LRUShard, TempFrameState};
use crate::{
  disk::{PagePool, PAGE_SIZE},
  utils::{ShortenedMutex, ToArc},
};

pub struct Shard {
  lru: LRUShard<usize, Arc<FrameState>>,
  eviction: BTreeSet<usize>, // evicting indexes
  temporary: BTreeMap<usize, Arc<TempFrameState>>, // temporary pages for gc without promote
}
impl Shard {
  pub fn remove_temp(&mut self, index: usize) {
    self.temporary.remove(&index);
  }
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
    self.state.get_frame_id()
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
      self.state.completion_evict(1);
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
    self.state.completion_evict(0);
  }
}

pub struct TempGuard<'a> {
  state: Arc<TempFrameState>,
  shard: &'a Mutex<Shard>,
}
impl<'a> TempGuard<'a> {
  fn new(state: Arc<TempFrameState>, shard: &'a Mutex<Shard>) -> Self {
    Self { state, shard }
  }
  pub fn take(self) -> (Arc<TempFrameState>, &'a Mutex<Shard>) {
    (self.state, self.shard)
  }
}

pub enum Acquired<'a> {
  Temp(TempGuard<'a>),
  Hit(Arc<FrameState>),
  Evicted(EvictionGuard<'a>),
}
pub enum Peeked<'a> {
  Hit(Arc<FrameState>),
  Temp(TempGuard<'a>),
  DiskRead(TempGuard<'a>),
}

pub struct LRUTable {
  shards: Vec<Mutex<Shard>>,
  offset: Vec<usize>,
  hasher: RandomState,
  page_pool: Arc<PagePool<PAGE_SIZE>>,
}
impl LRUTable {
  pub fn new(
    page_pool: Arc<PagePool<PAGE_SIZE>>,
    shard_count: usize,
    capacity: usize,
  ) -> Self {
    let cap_per_shard = capacity / shard_count;
    let mut shards = Vec::with_capacity(shard_count);
    let mut offset = Vec::with_capacity(shard_count);
    for i in 0..shard_count {
      let shard = Shard {
        lru: LRUShard::new(cap_per_shard),
        eviction: BTreeSet::new(),
        temporary: BTreeMap::new(),
      };
      shards.push(Mutex::new(shard));
      offset.push(i * cap_per_shard);
    }

    Self {
      shards,
      offset,
      hasher: Default::default(),
      page_pool,
    }
  }
  fn get_shard(&self, key: usize) -> (u64, &Mutex<Shard>, usize) {
    let h = self.hasher.hash_one(key);
    let i = h as usize % self.shards.len();
    let shard = &self.shards[i];
    let offset = self.offset[i];
    (h, shard, offset)
  }

  pub fn peek_or_temp<'a>(&'a self, index: usize) -> Peeked<'a> {
    let (hash, s, _) = self.get_shard(index);
    let backoff = Backoff::new();

    loop {
      let mut shard = s.l();
      if shard.eviction.contains(&index) {
        drop(shard);
        backoff.snooze();
        continue;
      }

      if let Some(state) = shard.temporary.get(&index) {
        if state.try_pin() {
          return Peeked::Temp(TempGuard::new(state.clone(), s));
        }

        drop(shard);
        backoff.snooze();
        continue;
      }

      if let Some(state) = shard.lru.peek(&index, hash) {
        if state.try_pin() {
          return Peeked::Hit(state.clone());
        }

        drop(shard);
        backoff.snooze();
        continue;
      }

      let state = shard
        .temporary
        .entry(index)
        .insert_entry(TempFrameState::new(self.page_pool.acquire()).to_arc())
        .get()
        .clone();

      return Peeked::DiskRead(TempGuard::new(state, s));
    }
  }

  pub fn acquire<'a>(&'a self, index: usize) -> Acquired<'a> {
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

      if let Some(state) = shard.temporary.get(&index) {
        if state.try_pin() {
          return Acquired::Temp(TempGuard::new(state.clone(), s));
        }

        drop(shard);
        backoff.snooze();
        continue;
      }

      if let Some(state) = shard.lru.get(&index, hash, hasher) {
        if state.try_pin() {
          return Acquired::Hit(state.clone());
        }

        drop(shard);
        backoff.snooze();
        continue;
      }

      if !shard.lru.is_full() {
        let frame_id = shard.lru.len() + offset;
        let state = FrameState::new(frame_id).to_arc();
        shard.lru.insert(index, state.clone(), hash, hasher);
        return Acquired::Evicted(EvictionGuard::new(
          None, state, &s, hasher, index, hash,
        ));
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
      return Acquired::Evicted(EvictionGuard::new(
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
