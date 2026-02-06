use std::{
  borrow::Borrow,
  hash::{BuildHasher, Hash},
  ops::Mul,
  ptr::NonNull,
};

use super::{Bucket, LRUList};
use hashbrown::{raw::RawTable, Equivalent};

fn equivalent<'a, K, V, Q: ?Sized + Equivalent<K>>(
  key: &'a Q,
) -> impl Fn(&NonNull<Bucket<K, V>>) -> bool + 'a {
  move |&ptr| key.equivalent(unsafe { ptr.as_ref() }.get_key())
}

fn make_hasher<'a, K, V, S>(
  hash_builder: &'a S,
) -> impl Fn(&NonNull<Bucket<K, V>>) -> u64 + 'a
where
  K: Hash,
  S: BuildHasher,
{
  move |&ptr| hash_builder.hash_one(unsafe { ptr.as_ref() }.get_key())
}

pub struct LRUShard<K, V> {
  old_entries: RawTable<NonNull<Bucket<K, V>>>,
  old_sub_list: LRUList<K, V>,
  new_entries: RawTable<NonNull<Bucket<K, V>>>,
  new_sub_list: LRUList<K, V>,
  capacity: usize,
}

impl<K, V> LRUShard<K, V> {
  pub fn new(capacity: usize) -> Self {
    Self {
      old_entries: RawTable::new(),
      old_sub_list: LRUList::new(),
      new_entries: RawTable::new(),
      new_sub_list: LRUList::new(),
      capacity,
    }
  }

  pub fn clear(&mut self) {
    self.old_entries.clear();
    self.old_sub_list.clear();
    self.new_entries.clear();
    self.new_sub_list.clear();
  }
}
impl<K, V> LRUShard<K, V>
where
  K: Eq + Hash,
{
  #[inline]
  fn get_bucket<Q: ?Sized, S>(
    &mut self,
    key: &Q,
    hash: u64,
    hasher: &S,
  ) -> Option<&mut Bucket<K, V>>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
    S: BuildHasher,
  {
    if let Some(bucket) = self.new_entries.get_mut(hash, equivalent(key)) {
      self.new_sub_list.move_to_head(bucket);
      return Some(unsafe { bucket.as_mut() });
    }

    let mut bucket = self.old_entries.remove_entry(hash, equivalent(key))?;
    self.old_sub_list.remove(&mut bucket);
    self.new_sub_list.push_head(&mut bucket);
    self.new_entries.insert(hash, bucket, make_hasher(hasher));
    self.rebalance(hasher);
    Some(unsafe { bucket.as_mut() })
  }
  pub fn get<Q: ?Sized, S>(&mut self, key: &Q, hash: u64, hasher: &S) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
    S: BuildHasher,
  {
    Some(self.get_bucket(key, hash, hasher)?.get_value())
  }
  pub fn get_mut<Q: ?Sized, S>(
    &mut self,
    key: &Q,
    hash: u64,
    hasher: &S,
  ) -> Option<&mut V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
    S: BuildHasher,
  {
    Some(self.get_bucket(key, hash, hasher)?.get_value_mut())
  }

  pub fn peek<Q: ?Sized>(&self, key: &Q, hash: u64) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    if let Some(bucket) = self.new_entries.get(hash, equivalent(key)) {
      return Some(unsafe { bucket.as_ref() }.get_value());
    }

    let bucket = match self.old_entries.get(hash, equivalent(key)) {
      Some(b) => b,
      None => return None,
    };
    Some(unsafe { bucket.as_ref() }.get_value())
  }

  fn rebalance<S>(&mut self, hasher: &S)
  where
    S: BuildHasher,
  {
    while self.new_sub_list.len().mul(3) > self.old_sub_list.len().mul(5) {
      let key = match self.new_sub_list.pop_tail() {
        Some(bucket) => unsafe { bucket.as_ref() }.get_key(),
        None => return,
      };
      let h = hasher.hash_one(key);
      let mut bucket = self.new_entries.remove_entry(h, equivalent(key)).unwrap();
      self.new_sub_list.remove(&mut bucket);
      self.old_sub_list.push_head(&mut bucket);
      self.old_entries.insert(h, bucket, make_hasher(hasher));
    }
  }

  fn evict<S>(&mut self, hasher: &S) -> Vec<(K, V)>
  where
    S: BuildHasher,
  {
    let mut evicted = Vec::new();
    while self.len() > self.capacity {
      let key = match self.old_sub_list.pop_tail() {
        Some(bucket) => unsafe { bucket.as_ref() }.get_key(),
        None => break,
      };
      let h = hasher.hash_one(key);
      let bucket = self
        .old_entries
        .remove_entry(h, equivalent(key))
        .map(|ptr| unsafe { Box::from_raw(ptr.as_ptr()) })
        .unwrap();
      self.rebalance(hasher);
      evicted.push(bucket.take())
    }

    evicted
  }

  pub fn insert<S>(
    &mut self,
    key: K,
    value: V,
    hash: u64,
    hasher: &S,
  ) -> (*mut V, Option<Vec<(K, V)>>)
  where
    S: BuildHasher,
  {
    if let Some(bucket) = self.new_entries.get_mut(hash, equivalent(&key)) {
      unsafe { bucket.as_mut() }.set_value(value);
      let ptr = unsafe { bucket.as_mut() }.get_value_mut() as *mut V;
      self.new_sub_list.move_to_head(bucket);
      return (ptr, None);
    }

    if let Some(mut bucket) = self.old_entries.remove_entry(hash, equivalent(&key)) {
      self.old_sub_list.remove(&mut bucket);
      unsafe { bucket.as_mut() }.set_value(value);
      let ptr = unsafe { bucket.as_mut() }.get_value_mut() as *mut V;

      let evicted = self.evict(hasher);

      self.new_sub_list.push_head(&mut bucket);
      self.new_entries.insert(hash, bucket, make_hasher(hasher));
      self.rebalance(hasher);
      if evicted.is_empty() {
        return (ptr, None);
      }

      return (ptr, Some(evicted));
    }

    let mut bucket = Bucket::new_ptr(key, value);
    let ptr = unsafe { bucket.as_mut() }.get_value_mut() as *mut V;
    let evicted = self.evict(hasher);
    self.old_sub_list.push_head(&mut bucket);
    self.old_entries.insert(hash, bucket, make_hasher(hasher));
    if evicted.is_empty() {
      return (ptr, None);
    }

    (ptr, Some(evicted))
  }

  pub fn remove<Q, S>(&mut self, key: &Q, hash: u64, hasher: &S) -> Option<V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
    S: BuildHasher,
  {
    if let Some(mut bucket) = self.new_entries.remove_entry(hash, equivalent(key)) {
      self.new_sub_list.remove(&mut bucket);
      let taken = unsafe { Box::from_raw(bucket.as_ptr()) };
      return Some(taken.take_value());
    }

    let mut bucket = match self.old_entries.remove_entry(hash, equivalent(key)) {
      Some(bucket) => bucket,
      None => return None,
    };
    self.old_sub_list.remove(&mut bucket);
    self.rebalance(hasher);
    let bucket = unsafe { Box::from_raw(bucket.as_ptr()) };
    Some(bucket.take_value())
  }

  pub fn has<Q: ?Sized>(&self, key: &Q, hash: u64) -> bool
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    self.new_entries.find(hash, equivalent(key)).is_some()
      || self.old_entries.find(hash, equivalent(key)).is_some()
  }

  pub fn len(&self) -> usize {
    self.new_entries.len() + self.old_entries.len()
  }
}
