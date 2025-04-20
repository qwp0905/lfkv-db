use std::ops::{Mul, Sub};

pub struct IterableBitMap {
  bits: Vec<u64>,
  active: Vec<usize>,
  removed: usize,
}
impl IterableBitMap {
  pub fn new() -> Self {
    Self {
      bits: vec![0; 1],
      active: Vec::new(),
      removed: 0,
    }
  }

  pub fn insert(&mut self, i: usize) {
    let index = i >> 6;
    let bit = i & 63;
    if index >= self.bits.len() {
      self.bits.resize(self.bits.len().mul(2), 0);
    }
    let bi = 1 << bit;
    if self.bits[index] & bi != 0 {
      return;
    };
    self.bits[index] |= bi;
    self.active.push(i);
  }

  pub fn remove(&mut self, i: usize) {
    let index = i >> 6;
    let bit = i & 63;
    if index >= self.bits.len() {
      return;
    }
    let bi = 1 << bit;
    if self.bits[index] & bi == 0 {
      return;
    };
    self.bits[index] &= !bi;
    self.removed += 1;
  }

  pub fn contains(&self, i: usize) -> bool {
    let index = i >> 6;
    let bit = i & 63;
    if index >= self.bits.len() {
      return false;
    }
    self.bits[index] & (1 << bit) != 0
  }

  pub fn is_empty(&self) -> bool {
    self.active.len().sub(self.removed).eq(&0)
  }
}
impl Default for IterableBitMap {
  fn default() -> Self {
    Self::new()
  }
}
impl IntoIterator for IterableBitMap {
  type Item = usize;

  type IntoIter = BitMapIntoIter;

  fn into_iter(self) -> Self::IntoIter {
    BitMapIntoIter::new(self)
  }
}

pub struct BitMapIntoIter {
  bits: IterableBitMap,
  index: usize,
}
impl BitMapIntoIter {
  pub fn new(bits: IterableBitMap) -> Self {
    Self { bits, index: 0 }
  }
}
impl<'a> Iterator for BitMapIntoIter {
  type Item = usize;

  fn next(&mut self) -> Option<Self::Item> {
    let &i = match self.bits.active.get(self.index) {
      Some(i) => i,
      None => return None,
    };

    if !self.bits.contains(i) {
      return None;
    }
    self.index += 1;
    Some(i)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_insert() {
    let mut bits = IterableBitMap::new();
    bits.insert(0);
    bits.insert(1);
    bits.insert(63);
    bits.insert(64);
    bits.insert(65);
    assert_eq!(bits.bits.len(), 2);
    assert_eq!(bits.active.len(), 5);
    assert_eq!(bits.removed, 0);
  }

  #[test]
  fn test_contains() {
    let mut bits = IterableBitMap::new();
    bits.insert(0);
    bits.insert(1);
    bits.insert(63);
    bits.insert(64);
    bits.insert(65);
    assert!(bits.contains(0));
    assert!(bits.contains(1));
    assert!(bits.contains(63));
    assert!(bits.contains(64));
    assert!(bits.contains(65));
    assert!(!bits.contains(2));
    assert!(!bits.contains(62));
    assert!(!bits.contains(66));
  }

  #[test]
  fn test_remove() {
    let mut bits = IterableBitMap::new();
    bits.insert(0);
    bits.insert(1);
    bits.insert(63);
    bits.insert(64);
    bits.insert(65);
    assert_eq!(bits.active.len(), 5);
    assert_eq!(bits.removed, 0);
    bits.remove(0);
    assert_eq!(bits.active.len(), 5);
    assert_eq!(bits.removed, 1);
    assert!(!bits.contains(0));
  }

  #[test]
  fn test_iter() {
    let mut bits = IterableBitMap::new();
    bits.insert(0);
    bits.insert(1);
    bits.insert(63);
    bits.insert(64);
    bits.insert(65);
    let mut iter = bits.into_iter();
    assert_eq!(iter.next(), Some(0));
    assert_eq!(iter.next(), Some(1));
    assert_eq!(iter.next(), Some(63));
    assert_eq!(iter.next(), Some(64));
    assert_eq!(iter.next(), Some(65));
    assert_eq!(iter.next(), None);
  }

  #[test]
  fn test_is_empty() {
    let mut bits = IterableBitMap::new();
    assert!(bits.is_empty());
    bits.insert(0);
    assert!(!bits.is_empty());
    bits.remove(0);
    assert!(bits.is_empty());
  }
}
