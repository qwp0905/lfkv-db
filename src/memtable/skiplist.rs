use std::{borrow::Borrow, ptr::NonNull};

use crate::unsafe_ref;

pub struct SkipList<K, V> {
  head: Option<NonNull<Entry<K, V>>>,
  tail: Option<NonNull<Entry<K, V>>>,
}

impl<K, V> SkipList<K, V> {
  pub fn new() -> Self {
    Self {
      head: None,
      tail: None,
    }
  }

  pub fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: Eq + Ord,
  {
    self.head.map(unsafe_ref).and_then(|head| head.get(k))
  }

  pub fn iter(&self) -> SkipListIter<'_, K, V> {
    SkipListIter {
      current: self.head.map(unsafe_ref),
    }
  }
}

struct Entry<K, V> {
  key: K,
  value: V,
  head: Option<NonNull<Entry<K, V>>>,
  tail: Option<NonNull<Entry<K, V>>>,
  node: NonNull<Node<K, V>>,
}
impl<K, V> Entry<K, V> {
  fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: Eq + Ord,
  {
    unsafe { self.node.as_ref() }.get(k)
  }
}

struct Node<K, V> {
  head: Option<NonNull<Node<K, V>>>,
  tail: Option<NonNull<Node<K, V>>>,
  bottom: Option<NonNull<Node<K, V>>>,
  entry: NonNull<Entry<K, V>>,
}
impl<K, V> Node<K, V> {
  fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: Eq + Ord,
  {
    let entry = unsafe { self.entry.as_ref() };
    let next = match entry.key.borrow().cmp(k) {
      std::cmp::Ordering::Less => self.bottom,
      std::cmp::Ordering::Equal => return Some(entry.value.borrow()),
      std::cmp::Ordering::Greater => self.tail,
    };
    next.and_then(|e| unsafe { e.as_ref() }.get(k))
  }
}

impl<K, V> Default for SkipList<K, V> {
  fn default() -> Self {
    Self::new()
  }
}

pub struct SkipListIter<'a, K, V> {
  current: Option<&'a Entry<K, V>>,
}
impl<'a, K, V> Iterator for SkipListIter<'a, K, V> {
  type Item = &'a V;
  fn next(&mut self) -> Option<Self::Item> {
    self.current.map(|current| {
      let next = current.value.borrow();
      self.current = current.tail.map(unsafe_ref);
      next
    })
  }
}
