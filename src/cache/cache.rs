use std::{
  borrow::Borrow,
  hash::{BuildHasher, Hash, RandomState},
  sync::Mutex,
};

use crate::{Callable, ShortenedMutex};

use super::{evict::Evicted, shard::LRUShard};

pub struct CacheConfig {
  shard_count: usize,
  capacity: usize,
}

pub struct LRUCache<K, V, S = RandomState> {
  shards: Vec<Mutex<LRUShard<K, V>>>,
  hasher: S,
  index: Box<dyn Callable<u64, usize>>,
}
impl<K, V> LRUCache<K, V, RandomState> {
  pub fn new(config: CacheConfig) -> Self {
    let cap = config.capacity.div_euclid(config.shard_count);
    let mut shards = Vec::new();
    for _ in 0..config.shard_count {
      shards.push(Mutex::new(LRUShard::new(cap)));
    }

    let hasher = Default::default();

    if config.shard_count & (config.shard_count - 1) == 0 {
      return Self {
        shards,
        hasher,
        index: Box::new(move |h| h as usize & (config.shard_count - 1)),
      };
    };

    Self {
      shards,
      hasher,
      index: Box::new(move |h| h as usize % config.shard_count),
    }
  }
}
impl<K, V, S> LRUCache<K, V, S>
where
  K: Eq + Hash,
  S: BuildHasher,
{
  fn get_shard<Q: ?Sized>(&self, key: &Q) -> (u64, &Mutex<LRUShard<K, V>>)
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    let h = self.hasher.hash_one(key);
    (h, &self.shards[self.index.call(h)])
  }

  pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<V>
  where
    K: Borrow<Q>,
    V: Clone,
    Q: Eq + Hash,
  {
    let (h, shard) = self.get_shard(key);
    let mut cache = shard.l();
    cache.get(key, h, &self.hasher).map(|v| v.clone())
  }

  pub fn insert(&self, key: K, value: V) -> Option<Evicted<'_, K, V>> {
    let (h, shard) = self.get_shard(&key);
    let mut cache = shard.l();
    match cache.insert(key, value, h, &self.hasher) {
      Some(evicted) => Some(Evicted::new(cache, evicted)),
      None => None,
    }
  }

  pub fn remove<Q>(&self, key: &Q) -> Option<V>
  where
    K: Borrow<Q>,
    Q: Eq + Hash,
  {
    let (h, shard) = self.get_shard(key);
    let mut cache = shard.l();
    cache.remove(key, h, &self.hasher)
  }

  pub fn peek<Q>(&self, key: &Q) -> Option<V>
  where
    K: Borrow<Q>,
    V: Clone,
    Q: Eq + Hash,
  {
    let (h, shard) = self.get_shard(key);
    let cache = shard.l();
    cache.peek(key, h).map(|v| v.clone())
  }

  pub fn has<Q>(&self, key: &Q) -> bool
  where
    K: Borrow<Q>,
    Q: Eq + Hash,
  {
    let (h, shard) = self.get_shard(key);
    let cache = shard.l();
    cache.has(key, h)
  }

  pub fn len(&self) -> usize {
    let mut locks = Vec::with_capacity(self.shards.len());
    let mut len = 0;
    for shard in &self.shards {
      let l = shard.l();
      len += l.len();
      locks.push(l);
    }

    len
  }

  pub fn clear(&self) {
    let mut locks = Vec::with_capacity(self.shards.len());
    for shard in &self.shards {
      let mut s = shard.l();
      s.clear();
      locks.push(s);
    }
  }
}
