use std::sync::MutexGuard;

use super::shard::LRUShard;

pub struct Evicted<'a, K, V> {
  guard: MutexGuard<'a, LRUShard<K, V>>,
  evicted: Vec<(K, V)>,
}
impl<'a, K, V> Evicted<'a, K, V> {
  pub fn new(guard: MutexGuard<'a, LRUShard<K, V>>, evicted: Vec<(K, V)>) -> Self {
    Self { guard, evicted }
  }

  pub fn pop(&mut self) -> Option<(K, V)> {
    self.evicted.pop()
  }
}
