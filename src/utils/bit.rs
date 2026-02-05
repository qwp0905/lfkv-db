const SHIFT: usize = 6;
const MAX_BIT: usize = 1 << SHIFT;
const MASK: usize = MAX_BIT - 1;

pub struct BitMap {
  bits: Vec<u64>,
  len_: usize,
}
impl BitMap {
  pub fn new(capacity: usize) -> Self {
    BitMap {
      bits: vec![0; capacity],
      len_: 0,
    }
  }

  pub fn insert(&mut self, n: usize) -> bool {
    let i = n >> SHIFT;
    let j = n & MASK;
    if i >= self.bits.len() {
      return false;
    };

    let prev = self.bits[i];
    self.bits[i] |= 1 << j;
    if prev == self.bits[i] {
      return false;
    };

    self.len_ += 1;
    true
  }

  pub fn contains(&mut self, n: usize) -> bool {
    let i = n >> SHIFT;
    let j = n & MASK;
    if i >= self.bits.len() {
      return false;
    };
    self.bits[i] & (1 << j) != 0
  }

  pub fn remove(&mut self, n: usize) -> bool {
    let i = n >> SHIFT;
    let j = n & MASK;
    if i >= self.bits.len() {
      return false;
    };

    let prev = self.bits[i];
    self.bits[i] &= !(1 << j);
    if prev == self.bits[i] {
      return false;
    }

    self.len_ -= 1;
    true
  }

  pub fn is_empty(&self) -> bool {
    self.len_ == 0
  }

  pub fn len(&self) -> usize {
    self.len_
  }

  pub fn iter(&self) -> BitMapIter<'_> {
    BitMapIter::new(&self.bits)
  }
}

pub struct BitMapIter<'a> {
  bits: &'a [u64],
  index: usize,
  remaining: u64,
}
impl<'a> BitMapIter<'a> {
  pub fn new(bits: &'a Vec<u64>) -> Self {
    let remaining = *bits.first().unwrap_or(&0);
    Self {
      bits,
      index: 0,
      remaining,
    }
  }
}

impl<'a> Iterator for BitMapIter<'a> {
  type Item = usize;

  fn next(&mut self) -> Option<Self::Item> {
    while self.remaining == 0 {
      self.index += 1;
      if self.index >= self.bits.len() {
        return None;
      };
      self.remaining = self.bits[self.index];
    }

    let bit = self.remaining.trailing_zeros() as usize;
    self.remaining &= self.remaining - 1;
    Some((self.index << SHIFT) + bit)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_insert() {
    let mut bits = BitMap::new(100);
    assert!(bits.insert(0));
    assert!(bits.insert(1));
    assert!(bits.insert(63));
    assert!(bits.insert(64));
    assert!(bits.insert(65));
  }

  #[test]
  fn test_contains() {
    let mut bits = BitMap::new(100);
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
    let mut bits = BitMap::new(100);
    assert!(bits.insert(0));
    assert!(bits.insert(1));
    assert!(bits.insert(63));
    assert!(bits.insert(64));
    assert!(bits.insert(65));
    assert_eq!(bits.len(), 5);
    assert!(bits.remove(0));
    assert_eq!(bits.len(), 4);
    assert!(!bits.contains(0));
  }

  #[test]
  fn test_iter() {
    let mut bits = BitMap::new(100);
    bits.insert(0);
    bits.insert(1);
    bits.insert(63);
    bits.insert(64);
    bits.insert(65);
    let mut iter = bits.iter();
    assert_eq!(iter.next(), Some(0));
    assert_eq!(iter.next(), Some(1));
    assert_eq!(iter.next(), Some(63));
    assert_eq!(iter.next(), Some(64));
    assert_eq!(iter.next(), Some(65));
    assert_eq!(iter.next(), None);
  }

  #[test]
  fn test_is_empty() {
    let mut bits = BitMap::new(100);
    assert!(bits.is_empty());
    bits.insert(0);
    assert!(!bits.is_empty());
    bits.remove(0);
    assert!(bits.is_empty());
  }
}
