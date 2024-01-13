use std::{
  borrow::Borrow,
  collections::hash_map::RandomState,
  hash::{BuildHasher, Hash, Hasher},
  mem::replace,
  ptr::NonNull,
};

use hashbrown::{raw::RawTable, Equivalent};

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
pub struct Cache<K, V, S = RandomState> {
  raw: RawTable<Pointer<Bucket<K, V>>>,
  entries: Entries<K, V>,
  hasher: S,
  capacity: usize,
}
#[allow(unused)]
impl<K, V, S> Cache<K, V, S> {
  pub fn with_hasher(hasher: S, capacity: usize) -> Self {
    Self {
      raw: Default::default(),
      entries: Default::default(),
      hasher,
      capacity,
    }
  }
}

impl<K, V> Cache<K, V, RandomState> {
  pub fn new(capacity: usize) -> Cache<K, V> {
    Cache {
      raw: Default::default(),
      entries: Default::default(),
      hasher: Default::default(),
      capacity,
    }
  }
}

#[allow(unused)]
impl<K, V, S> Cache<K, V, S>
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

  pub fn insert(&mut self, k: K, v: V) -> Option<(K, V)> {
    let h = hash(&k, &self.hasher);
    let eq = equivalent(&k);
    let hasher = make_hasher(&self.hasher);
    unsafe {
      match self.raw.find_or_find_insert_slot(h, eq, hasher) {
        Ok(i) => {
          let e = i.as_mut();
          self.entries.move_back(e);
          let bucket = e.as_mut().as_mut();
          drop(replace(&mut bucket.value, v));
          return None;
        }
        Err(slot) => {
          let bucket = Bucket {
            key: k,
            value: v,
            status: Status::Old,
          };
          let pointer = DoubleLinkedListElement::new_ptr(bucket);
          self.raw.insert_in_slot(h, slot, pointer.to_owned());
          self.entries.add(pointer);

          if self.raw.len() <= self.capacity {
            return None;
          };

          if let Some(b) = self.entries.oldest() {
            let dh = hash(&b.key, &self.hasher);
            let deq = equivalent(&b.key);
            return self.raw.remove_entry(dh, deq).map(|ptr| {
              self.entries.remove(ptr);
              let bucket = Box::from_raw(ptr.as_ptr()).element;
              return (bucket.key, bucket.value);
            });
          };

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
}
unsafe impl<K, V> Send for Cache<K, V>
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
      self.new.push_back(e.to_owned());
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

  fn old_pop(&mut self) -> Option<Bucket<K, V>> {
    self.old.pop_front().map(|ptr| ptr.element)
  }

  fn oldest(&self) -> Option<&Bucket<K, V>> {
    self.old.front()
  }
}

#[cfg(test)]
mod tests {
  use super::Cache;

  #[test]
  fn _1() {
    let mut p = Cache::<usize, usize>::new(2);
    assert_eq!(p.insert(1, 1), None);
    assert_eq!(p.insert(2, 2), None);
    assert_eq!(p.insert(3, 3), Some((1, 1)));
  }

  #[test]
  fn _2() {
    let mut p = Cache::<usize, usize>::new(5);
    assert_eq!(p.insert(1, 1), None);
    assert_eq!(p.insert(2, 2), None);
    assert_eq!(p.insert(3, 3), None);
    assert_eq!(p.insert(4, 4), None);
    assert_eq!(p.insert(5, 5), None);
    assert_eq!(p.insert(6, 6), Some((1, 1)));
    assert_eq!(p.insert(7, 7), Some((2, 2)));
    assert_eq!(p.insert(8, 8), Some((3, 3)));
    assert_eq!(p.get(&1), None);
    assert_eq!(p.get(&3), None);
    assert_eq!(p.get(&5), Some(&5));
    assert_eq!(p.get(&7), Some(&7));
    assert_eq!(p.get(&4), Some(&4));
    assert_eq!(p.get(&9), None);
    assert_eq!(p.remove(&2), None);
    assert_eq!(p.remove(&7), Some(7));
    assert_eq!(p.insert(1, 1), None);
    assert_eq!(p.insert(2, 2), Some((6, 6)));
    assert_eq!(p.insert(3, 3), Some((8, 8)));
    assert_eq!(p.insert(6, 6), Some((1, 1)));
    assert_eq!(p.insert(8, 8), Some((2, 2)));
    assert_eq!(p.insert(1, 1), Some((3, 3)));
  }

  #[test]
  fn _3() {
    let mut p = Cache::<usize, usize>::new(1);
    assert_eq!(p.insert(1, 1), None);
    assert_eq!(p.insert(2, 2), Some((1, 1)));
    assert_eq!(p.remove(&1), None);
  }

  #[test]
  fn _4() {
    #[derive(Debug, PartialEq, Eq)]
    struct T {
      i: usize,
    }

    let mut p = Cache::<usize, T>::new(10);
    assert_eq!(p.insert(123, T { i: 1 }), None);
    assert_eq!(p.get(&123), Some(&T { i: 1 }));
  }
}
