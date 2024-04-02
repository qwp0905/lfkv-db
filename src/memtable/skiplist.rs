use std::{borrow::Borrow, ptr::NonNull};

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
    match self.head.map(|h| unsafe { h.as_ref() }) {
      Some(head) => {
        if head.key.borrow().lt(k) {
          return None;
        };
        return head.get(k);
      }
      None => None,
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
    match unsafe { self.node.as_ref() }.get(k) {
      Ok(v) => return Some(v),
      Err(o) => match o {
        Some(node) => todo!(),
        None => todo!(),
      },
    }
  }
}

struct Node<K, V> {
  head: Option<NonNull<Node<K, V>>>,
  tail: Option<NonNull<Node<K, V>>>,
  bottom: Option<NonNull<Node<K, V>>>,
  entry: NonNull<Entry<K, V>>,
}
impl<K, V> Node<K, V> {
  fn get<Q: ?Sized>(&self, k: &Q) -> Result<&V, Option<NonNull<Node<K, V>>>>
  where
    K: Borrow<Q>,
    Q: Eq + Ord,
  {
    let entry = unsafe { self.entry.as_ref() };
    match entry.key.borrow().cmp(k) {
      std::cmp::Ordering::Less => Err(self.bottom),
      std::cmp::Ordering::Equal => Ok(entry.value.borrow()),
      std::cmp::Ordering::Greater => Err(self.tail),
    }
  }
}
