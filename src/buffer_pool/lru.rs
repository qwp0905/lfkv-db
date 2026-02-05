use std::{
  borrow::Borrow,
  hash::{BuildHasher, Hash, RandomState},
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

pub struct LRUTable<K, V, S = RandomState> {
  old_entries: RawTable<NonNull<Bucket<K, V>>>,
  old_sub_list: LRUList<K, V>,
  new_entries: RawTable<NonNull<Bucket<K, V>>>,
  new_sub_list: LRUList<K, V>,
  capacity: usize,
  hasher: S,
}

impl<K, V> LRUTable<K, V> {
  pub fn new(capacity: usize) -> Self {
    Self {
      old_entries: RawTable::new(),
      old_sub_list: LRUList::new(),
      new_entries: RawTable::new(),
      new_sub_list: LRUList::new(),
      capacity,
      hasher: Default::default(),
    }
  }

  pub fn clear(&mut self) {
    self.old_entries.clear();
    self.old_sub_list.clear();
    self.new_entries.clear();
    self.new_sub_list.clear();
  }
}
impl<K, V, S> LRUTable<K, V, S>
where
  K: Eq + Hash,
  S: BuildHasher,
{
  #[inline]
  fn get_bucket<Q: ?Sized>(&mut self, key: &Q) -> Option<&mut Bucket<K, V>>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
    S: BuildHasher,
  {
    let hash = self.hasher.hash_one(key);
    if let Some(bucket) = self.new_entries.get_mut(hash, equivalent(key)) {
      self.new_sub_list.move_to_head(bucket);
      return Some(unsafe { bucket.as_mut() });
    }

    let mut bucket = self.old_entries.remove_entry(hash, equivalent(key))?;
    self.old_sub_list.remove(&mut bucket);
    self.new_sub_list.push_head(&mut bucket);
    self
      .new_entries
      .insert(hash, bucket, make_hasher(&self.hasher));
    self.rebalance();
    Some(unsafe { bucket.as_mut() })
  }
  pub fn get<Q: ?Sized>(&mut self, key: &Q) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    Some(self.get_bucket(key)?.get_value())
  }
  pub fn get_mut<Q: ?Sized>(&mut self, key: &Q) -> Option<&mut V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
    S: BuildHasher,
  {
    Some(self.get_bucket(key)?.get_value_mut())
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

  fn rebalance(&mut self)
  where
    S: BuildHasher,
  {
    while self.new_sub_list.len().mul(3) > self.old_sub_list.len().mul(5) {
      let key = match self.new_sub_list.pop_tail() {
        Some(bucket) => unsafe { bucket.as_ref() }.get_key(),
        None => break,
      };
      let h = self.hasher.hash_one(key);
      let mut bucket = self.new_entries.remove_entry(h, equivalent(key)).unwrap();
      self.new_sub_list.remove(&mut bucket);
      self.old_sub_list.push_head(&mut bucket);
      self
        .old_entries
        .insert(h, bucket, make_hasher(&self.hasher));
    }
  }

  pub fn evict(&mut self) -> Option<(K, V)>
  where
    S: BuildHasher,
  {
    let key = unsafe { self.old_sub_list.pop_tail()?.as_ref() }.get_key();
    let h = self.hasher.hash_one(key);
    let bucket = self
      .old_entries
      .remove_entry(h, equivalent(key))
      .map(|ptr| unsafe { Box::from_raw(ptr.as_ptr()) })?;
    self.rebalance();
    Some(bucket.take())
  }

  pub fn insert(&mut self, key: K, value: V) -> Option<V> {
    let hash = self.hasher.hash_one(&key);
    if let Some(bucket) = self.new_entries.get_mut(hash, equivalent(&key)) {
      let prev = unsafe { bucket.as_mut() }.set_value(value);
      self.new_sub_list.move_to_head(bucket);
      return Some(prev);
    }

    if let Some(mut bucket) = self.old_entries.remove_entry(hash, equivalent(&key)) {
      self.old_sub_list.remove(&mut bucket);
      let prev = unsafe { bucket.as_mut() }.set_value(value);

      self.new_sub_list.push_head(&mut bucket);
      self
        .new_entries
        .insert(hash, bucket, make_hasher(&self.hasher));
      self.rebalance();

      return Some(prev);
    }

    let mut bucket = Bucket::new_ptr(key, value);
    self.old_sub_list.push_head(&mut bucket);
    self
      .old_entries
      .insert(hash, bucket, make_hasher(&self.hasher));
    None
  }

  pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    let hash = self.hasher.hash_one(key);
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
    self.rebalance();
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

  pub fn is_full(&self) -> bool {
    self.len() == self.capacity
  }
}
