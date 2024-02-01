use std::{
  borrow::Borrow,
  collections::hash_map::RandomState,
  hash::{BuildHasher, Hash, Hasher},
  mem::replace,
  ptr::NonNull,
};

use hashbrown::{
  raw::{InsertSlot, RawTable},
  Equivalent,
};

use super::list::{DoubleLinkedList, DoubleLinkedListElement};

type Pointer<T> = NonNull<DoubleLinkedListElement<T>>;

#[derive(Debug)]
enum Status {
  Old,
  New,
}

#[derive(Debug)]
struct Bucket<K, V> {
  key: K,
  value: V,
  status: Status,
}
impl<K, V> AsRef<V> for Bucket<K, V> {
  fn as_ref(&self) -> &V {
    &self.value
  }
}
impl<K, V> AsMut<V> for Bucket<K, V> {
  fn as_mut(&mut self) -> &mut V {
    &mut self.value
  }
}

#[allow(unused)]
pub struct LRUCache<K, V, S = RandomState> {
  raw: RawTable<Pointer<Bucket<K, V>>>,
  entries: Entries<K, V>,
  hasher: S,
}
#[allow(unused)]
impl<K, V, S> LRUCache<K, V, S> {
  pub fn with_hasher(hasher: S, capacity: usize) -> Self {
    Self {
      raw: Default::default(),
      entries: Default::default(),
      hasher,
    }
  }
}

impl<K, V> LRUCache<K, V, RandomState> {
  pub fn new() -> LRUCache<K, V> {
    LRUCache {
      raw: Default::default(),
      entries: Default::default(),
      hasher: Default::default(),
    }
  }

  pub fn with_capacity(capacity: usize) -> Self {
    todo!()
  }
}
impl<K, V> Default for LRUCache<K, V, RandomState> {
  fn default() -> Self {
    Self::new()
  }
}

#[allow(unused)]
impl<K, V, S> LRUCache<K, V, S>
where
  K: Eq + Hash,
  S: BuildHasher,
{
  pub fn get<Q: ?Sized>(&mut self, k: &Q) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    let h = hash(k, &self.hasher);
    let eq = equivalent(k);
    self.raw.get_mut(h, eq).map(|e| {
      self.entries.move_back(e);
      unsafe { e.as_ref() }.element().as_ref()
    })
  }

  pub fn get_mut<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    let h = hash(k, &self.hasher);
    let eq = equivalent(k);
    self
      .raw
      .get_mut(h, eq)
      .map(|e| unsafe { e.as_mut() }.element.as_mut())
  }

  pub fn insert(&mut self, k: K, v: V) -> Option<V> {
    let h = hash(&k, &self.hasher);
    let eq = equivalent(&k);
    let hasher = make_hasher(&self.hasher);
    unsafe {
      match self.raw.find_or_find_insert_slot(h, eq, hasher) {
        Ok(i) => {
          let e = i.as_mut();
          self.entries.move_back(e);
          let bucket = e.as_mut().as_mut();
          return Some(replace(&mut bucket.value, v));
        }
        Err(slot) => {
          let pointer = DoubleLinkedListElement::new_ptr(Bucket {
            key: k,
            value: v,
            status: Status::Old,
          });
          self.raw.insert_in_slot(h, slot, pointer.to_owned());
          self.entries.add(pointer);
          return None;
        }
      }
    }
  }

  pub fn remove<Q>(&mut self, k: &Q) -> Option<V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    let h = hash(k, &self.hasher);
    let eq = equivalent(k);
    return self.raw.remove_entry(h, eq).map(|ptr| {
      self.entries.remove(ptr);
      let bucket = unsafe { Box::from_raw(ptr.as_ptr()) }.element;
      return bucket.value;
    });
  }

  pub fn len(&self) -> usize {
    self.raw.len()
  }

  pub fn pop_old(&mut self) -> Option<(K, V)> {
    if let Some(b) = self.entries.oldest() {
      let dh = hash(&b.key, &self.hasher);
      let deq = equivalent(&b.key);
      return self.raw.remove_entry(dh, deq).map(|ptr| {
        self.entries.remove(ptr);
        let bucket = unsafe { Box::from_raw(ptr.as_ptr()) }.element;
        return (bucket.key, bucket.value);
      });
    };

    return None;
  }

  pub fn entry(&mut self, k: K) -> CacheEntry<'_, K, V, S> {
    let h = hash(&k, &self.hasher);
    let eq = equivalent(&k);
    let hasher = make_hasher(&self.hasher);

    let status = unsafe {
      match self.raw.find_or_find_insert_slot(h, eq, hasher) {
        Ok(b) => EntryStatus::Occupied(b),
        Err(slot) => EntryStatus::Vacant(slot, k, h),
      }
    };
    CacheEntry {
      inner: self,
      status,
    }
  }

  pub fn peek_old(&self) -> Option<(&K, &V)> {
    self
      .entries
      .oldest()
      .map(|bucket| (&bucket.key, &bucket.value))
  }
}
unsafe impl<K, V> Send for LRUCache<K, V>
where
  K: Send,
  V: Send,
{
}

#[inline]
fn hash<Q, S>(val: &Q, hasher: &S) -> u64
where
  Q: Hash + ?Sized,
  S: BuildHasher,
{
  let mut state = hasher.build_hasher();
  val.hash(&mut state);
  state.finish()
}

#[allow(unused)]
fn equivalent<'a, K, V, Q: ?Sized + Equivalent<K>>(
  key: &'a Q,
) -> impl Fn(&Pointer<Bucket<K, V>>) -> bool + 'a {
  move |&ptr| {
    let bucket = unsafe { ptr.as_ref() }.element();
    key.equivalent(&bucket.key)
  }
}

fn make_hasher<'a, Q, V, S>(
  hash_builder: &'a S,
) -> impl Fn(&Pointer<Bucket<Q, V>>) -> u64 + 'a
where
  Q: Hash,
  S: BuildHasher,
{
  move |&ptr| {
    let bucket = unsafe { ptr.as_ref() }.element();
    hash(&bucket.key, hash_builder)
  }
}

#[derive(Debug)]
struct Entries<K, V> {
  old: DoubleLinkedList<Bucket<K, V>>,
  new: DoubleLinkedList<Bucket<K, V>>,
}
impl<K, V> Default for Entries<K, V> {
  fn default() -> Self {
    Self {
      old: DoubleLinkedList::new(),
      new: DoubleLinkedList::new(),
    }
  }
}
#[allow(unused)]
impl<K, V> Entries<K, V> {
  fn move_back(&mut self, e: &mut Pointer<Bucket<K, V>>) {
    let bucket = unsafe { e.as_mut() }.as_mut();
    unsafe {
      match bucket.status {
        Status::New => self.new.remove(*e),
        Status::Old => self.old.remove(*e),
      }
      self.new.push_back(*e);
    }
    bucket.status = Status::New;
    self.relocate();
  }

  fn relocate(&mut self) {
    while self.old.len() * 5 < self.new.len() * 3 {
      match self.new.pop_front() {
        None => break,
        Some(mut e) => {
          let bucket = e.as_mut().as_mut();
          bucket.status = Status::Old;
          unsafe { self.old.push_back(NonNull::from(Box::leak(e))) };
        }
      }
    }
  }

  fn add(&mut self, e: Pointer<Bucket<K, V>>) {
    unsafe { self.old.push_back(e) }
  }

  fn remove(&mut self, e: Pointer<Bucket<K, V>>) {
    unsafe {
      let bucket = e.as_ref().element();
      match bucket.status {
        Status::New => self.new.remove(e),
        Status::Old => self.old.remove(e),
      }
    }
    self.relocate();
  }

  fn pop_old(&mut self) -> Option<Bucket<K, V>> {
    self.old.pop_front().map(|ptr| ptr.element)
  }

  fn oldest(&self) -> Option<&Bucket<K, V>> {
    self.old.front()
  }
}

enum EntryStatus<K, V> {
  Vacant(InsertSlot, K, u64),
  Occupied(hashbrown::raw::Bucket<NonNull<DoubleLinkedListElement<Bucket<K, V>>>>),
}

#[allow(unused)]
pub struct CacheEntry<'a, K, V, S> {
  inner: &'a mut LRUCache<K, V, S>,
  status: EntryStatus<K, V>,
}
#[allow(unused)]
impl<'a, K, V, S> CacheEntry<'a, K, V, S>
where
  K: Eq + Hash,
  S: BuildHasher,
{
  pub fn and_modify<F>(self, f: F) -> Self
  where
    F: FnOnce(&mut V),
  {
    let v = match &self.status {
      EntryStatus::Vacant(_, _, _) => return self,
      EntryStatus::Occupied(v) => v,
    };
    unsafe {
      let ptr = v.as_mut();
      f(ptr.as_mut().as_mut().as_mut());
      self.inner.entries.move_back(ptr);
    };

    self
  }

  pub fn or_insert(self, v: V) -> &'a mut V {
    let (slot, k, h) = match self.status {
      EntryStatus::Occupied(v) => {
        return unsafe { v.as_mut().as_mut() }.as_mut().as_mut()
      }
      EntryStatus::Vacant(slot, k, h) => (slot, k, h),
    };

    let pointer = DoubleLinkedListElement::new_ptr(Bucket {
      key: k,
      value: v,
      status: Status::Old,
    });

    let b = unsafe { self.inner.raw.insert_in_slot(h, slot, pointer.to_owned()) };
    self.inner.entries.add(pointer);
    return unsafe { b.as_mut().as_mut() }.as_mut().as_mut();
  }
}

#[allow(unused)]
impl<'a, K, V, S> CacheEntry<'a, K, V, S>
where
  K: Eq + Hash,
  V: Default,
  S: BuildHasher,
{
  pub fn or_default(self) -> &'a mut V {
    self.or_insert(Default::default())
  }
}

// #[cfg(test)]
// mod tests {
//   use super::Cache;

//   #[test]
//   fn _1() {
//     let mut p = Cache::<usize, usize>::new();
//     assert_eq!(p.insert(1, 1), None);
//     assert_eq!(p.insert(2, 2), None);
//     assert_eq!(p.insert(3, 3), None);
//   }

//   #[test]
//   fn _2() {
//     let mut p = Cache::<usize, usize>::new();
//     assert_eq!(p.insert(1, 1), None);
//     assert_eq!(p.insert(2, 2), None);
//     assert_eq!(p.insert(3, 3), None);
//     assert_eq!(p.insert(4, 4), None);
//     assert_eq!(p.insert(5, 5), None);
//     assert_eq!(p.insert(6, 6), None);
//     assert_eq!(p.insert(7, 7), None);
//     assert_eq!(p.insert(8, 8), None);
//     assert_eq!(p.get(&1), None);
//     assert_eq!(p.get(&3), None);
//     assert_eq!(p.get(&5), Some(&5));
//     assert_eq!(p.get(&7), Some(&7));
//     assert_eq!(p.get(&4), Some(&4));
//     assert_eq!(p.get(&9), None);
//     assert_eq!(p.remove(&2), None);
//     assert_eq!(p.remove(&7), Some(7));
//     assert_eq!(p.insert(1, 1), None);
//     assert_eq!(p.insert(2, 2), None);
//     assert_eq!(p.insert(3, 3), None);
//     assert_eq!(p.insert(6, 6), None);
//     assert_eq!(p.insert(8, 8), None);
//     assert_eq!(p.insert(1, 1), None);
//   }

//   #[test]
//   fn _3() {
//     let mut p = Cache::<usize, usize>::new();
//     assert_eq!(p.insert(1, 1), None);
//     assert_eq!(p.insert(2, 2), None);
//     assert_eq!(p.remove(&1), None);
//   }

//   #[test]
//   fn _4() {
//     #[derive(Debug, PartialEq, Eq)]
//     struct T {
//       i: usize,
//     }

//     let mut p = Cache::<usize, T>::new();
//     assert_eq!(p.insert(123, T { i: 1 }), None);
//     assert_eq!(p.get(&123), Some(&T { i: 1 }));
//   }
// }
