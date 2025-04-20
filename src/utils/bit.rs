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
