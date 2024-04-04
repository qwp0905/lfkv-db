use std::{
  borrow::Borrow,
  collections::VecDeque,
  mem::replace,
  ops::Add,
  ptr::NonNull,
  sync::atomic::AtomicUsize,
  time::{SystemTime, UNIX_EPOCH},
};

use crate::{unsafe_ref, Pointer};

// pub struct SkipList<K, V> {
//   head: Option<NonNull<Entry<K, V>>>,
// }

pub struct SkipListL<K, V> {
  head: Head<K, V>,
}
impl<K, V> SkipListL<K, V> {
  pub fn new() -> Self {
    Self { head: Head::new() }
  }

  pub fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: Eq + Ord,
  {
    self
      .head
      .pointers
      .front()
      .and_then(|node| node.refs().find(k))
      .map(|entry| entry.value.borrow())
  }

  pub fn insert(&mut self, k: K, v: V)
  where
    K: Eq + Ord,
  {
    let height = self.random_height();
    let entry = Entry::new(k, v);
    let node = self.head.pointers.get_mut(height);
    loop {
      if let Some(n) = node {
        let ce = n.muts().entry.muts();
        match ce.key.cmp(entry.key.borrow()) {
          std::cmp::Ordering::Less => break,
          std::cmp::Ordering::Equal => {
            replace(&mut ce.value, entry.value);
            return;
          }
          std::cmp::Ordering::Greater => {
            if let Some(nn) = n.muts().next {
              *n = nn;
              continue;
            };
            break;
          }
        }
      }
    }
  }

  fn random_height(&mut self) -> usize {
    (SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .subsec_nanos() as usize)
      .rem_euclid(self.head.len().add(1))
  }
}

struct Head<K, V> {
  pointers: VecDeque<NonNull<Node<K, V>>>,
}
impl<K, V> Head<K, V> {
  fn new() -> Self {
    Self {
      pointers: VecDeque::new(),
    }
  }

  fn is_empty(&self) -> bool {
    self.pointers.is_empty()
  }

  fn len(&self) -> usize {
    self.pointers.len()
  }
}

struct Entry<K, V> {
  nodes: VecDeque<NonNull<Node<K, V>>>,
  key: K,
  value: V,
  next: Option<NonNull<Entry<K, V>>>,
  prev: Option<NonNull<Entry<K, V>>>,
}

impl<K, V> Entry<K, V> {
  fn new(key: K, value: V) -> Self {
    Self {
      nodes: Default::default(),
      key,
      value,
      next: None,
      prev: None,
    }
  }
}
struct Node<K, V> {
  next: Option<NonNull<Node<K, V>>>,
  prev: Option<NonNull<Node<K, V>>>,
  bottom: Option<NonNull<Node<K, V>>>,
  entry: NonNull<Entry<K, V>>,
}
impl<K, V> Node<K, V> {
  fn find<Q: ?Sized>(&self, k: &Q) -> Option<&Entry<K, V>>
  where
    K: Borrow<Q>,
    Q: Eq + Ord,
  {
    let entry = self.entry.refs();
    let next = match entry.key.borrow().cmp(k) {
      std::cmp::Ordering::Less => self.bottom,
      std::cmp::Ordering::Equal => return Some(entry),
      std::cmp::Ordering::Greater => self.next,
    };
    next.map(unsafe_ref).and_then(|e| e.find(k))
  }
}

// impl<K, V> SkipList<K, V> {
//   pub fn new() -> Self {
//     Self { head: None }
//   }

//   pub fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
//   where
//     K: Borrow<Q>,
//     Q: Eq + Ord,
//   {
//     self
//       .head
//       .map(unsafe_ref)
//       .and_then(|head| head.get(k).map(|entry| &entry.value))
//   }

//   pub fn insert(&mut self, key: K, value: V) {
//     match self.head {
//       Some(mut entry) => {}
//       None => {
//         self.head = Some(Entry::new_ptr(key, value));
//       }
//     }
//   }

//   pub fn iter(&self) -> SkipListIter<'_, K, V> {
//     SkipListIter {
//       current: self.head.map(unsafe_ref),
//     }
//   }
// }

// struct Entry<K, V> {
//   key: K,
//   value: V,
//   head: Option<NonNull<Entry<K, V>>>,
//   tail: Option<NonNull<Entry<K, V>>>,
//   node: NonNull<Node<K, V>>,
// }
// impl<K, V> Entry<K, V> {
//   fn new_ptr(key: K, value: V) -> NonNull<Self> {
//     let mut ptr = NonNull::from_box(Entry {
//       key,
//       value,
//       head: None,
//       tail: None,
//       node: NonNull::dangling(),
//     });

//     ptr.muts().node = NonNull::from_box(Node {
//       head: None,
//       tail: None,
//       bottom: None,
//       entry: ptr,
//     });
//     ptr
//   }

//   fn get<Q: ?Sized>(&self, k: &Q) -> Option<&Entry<K, V>>
//   where
//     K: Borrow<Q>,
//     Q: Eq + Ord,
//   {
//     self.node.refs().get(k)
//   }

//   fn find_slot<Q: ?Sized>(&self, k: &Q)
//   where
//     K: Borrow<Q>,
//     Q: Eq + Ord,
//   {
//     if self.key.borrow().eq(k) {}
//   }
// }

// struct Node<K, V> {
//   head: Option<NonNull<Node<K, V>>>,
//   tail: Option<NonNull<Node<K, V>>>,
//   bottom: Option<NonNull<Node<K, V>>>,
//   entry: NonNull<Entry<K, V>>,
// }
// impl<K, V> Node<K, V> {
//   fn get<Q: ?Sized>(&self, k: &Q) -> Option<&Entry<K, V>>
//   where
//     K: Borrow<Q>,
//     Q: Eq + Ord,
//   {
//     let entry = self.entry.refs();
//     let next = match entry.key.borrow().cmp(k) {
//       std::cmp::Ordering::Less => self.bottom,
//       std::cmp::Ordering::Equal => return Some(entry),
//       std::cmp::Ordering::Greater => self.tail,
//     };
//     next.map(unsafe_ref).and_then(|e| e.get(k))
//   }
// }

// impl<K, V> Default for SkipList<K, V> {
//   fn default() -> Self {
//     Self::new()
//   }
// }

// pub struct SkipListIter<'a, K, V> {
//   current: Option<&'a Entry<K, V>>,
// }
// impl<'a, K, V> Iterator for SkipListIter<'a, K, V> {
//   type Item = (&'a K, &'a V);
//   fn next(&mut self) -> Option<Self::Item> {
//     self.current.map(|current| {
//       let next = (&current.key, &current.value);
//       self.current = current.tail.map(unsafe_ref);
//       next
//     })
//   }
// }
