use std::{
  collections::hash_map::RandomState,
  hash::{BuildHasher, Hash},
  sync::RwLock,
};

use crate::ShortenedRwLock;

pub struct BloomFilter(RwLock<BloomFilterInner>);

struct BloomFilterInner {
  bit: Vec<bool>,
  hasher: Vec<RandomState>,
}

impl BloomFilter {
  pub fn new(size: usize, hash_func: usize) -> Self {
    Self(RwLock::new(BloomFilterInner {
      bit: (0..size).map(|_| false).collect(),
      hasher: (0..hash_func).map(|_| RandomState::new()).collect(),
    }))
  }

  pub fn set<K: Hash>(&mut self, k: K) {
    let mut inner = self.0.wl();
    let len = inner.bit.len();

    for i in 0..inner.hasher.len() {
      let h = (inner.hasher[i].hash_one(&k) as usize).rem_euclid(len);
      inner.bit[h] = true;
    }
  }

  pub fn get<K: Hash>(&self, k: K) -> bool {
    let inner = self.0.rl();
    let len = inner.bit.len();

    for builder in inner.hasher.iter() {
      let h = (builder.hash_one(&k) as usize).rem_euclid(len);
      if !inner.bit[h] {
        return false;
      }
    }

    return true;
  }
}
