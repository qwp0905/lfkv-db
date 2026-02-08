use std::ops::{Add, Sub};

use crate::{
  disk::{Page, Serializable},
  error::Error,
};

pub static MAX_NODE_LEN: usize = 12;
pub static MIN_NODE_LEN: usize = 6;

#[derive(Debug)]
pub enum CursorEntry {
  Internal(InternalNode),
  Leaf(LeafNode),
}
impl Serializable for CursorEntry {
  fn serialize(&self) -> Result<Page, Error> {
    match self {
      Self::Leaf(node) => node.serialize(),
      Self::Internal(node) => node.serialize(),
    }
  }
  fn deserialize(value: &Page) -> Result<Self, Error> {
    let mut sc = value.scanner();
    match sc.read()? {
      1 => Ok(Self::Leaf(value.deserialize()?)),
      2 => Ok(Self::Internal(value.deserialize()?)),
      _ => Err(Error::InvalidFormat),
    }
  }
}
impl CursorEntry {
  pub fn find_or_next(&self, key: &Vec<u8>) -> Result<usize, Option<usize>> {
    match self {
      Self::Internal(node) => Err(Some(node.next(key))),
      Self::Leaf(node) => match node.find(key) {
        Some(i) => Ok(i),
        None => Err(None),
      },
    }
  }

  pub fn as_leaf(&mut self) -> &mut LeafNode {
    match self {
      Self::Leaf(node) => node,
      _ => unreachable!(),
    }
  }

  pub fn as_internal(&mut self) -> &mut InternalNode {
    match self {
      Self::Internal(node) => node,
      _ => unreachable!(),
    }
  }

  pub fn top(&self) -> Vec<u8> {
    match self {
      Self::Leaf(node) => node.keys[0].0.clone(),
      Self::Internal(node) => node.keys[0].clone(),
    }
  }
}

#[derive(Debug)]
pub struct InternalNode {
  pub keys: Vec<Vec<u8>>,
  pub children: Vec<usize>,
}
impl InternalNode {
  pub fn split(&mut self) -> (CursorEntry, Vec<u8>) {
    let c = self.keys.len().div_ceil(2);
    let mut keys = self.keys.split_off(c);
    let m = keys.remove(0);
    let children = self.children.split_off(c.add(1));
    (CursorEntry::Internal(InternalNode { keys, children }), m)
  }

  pub fn pop_back(&mut self) -> Option<(Vec<u8>, usize)> {
    Some((self.keys.pop()?, self.children.pop()?))
  }
  pub fn pop_front(&mut self) -> Option<(Vec<u8>, usize)> {
    if self.len().eq(&0) {
      return None;
    }
    Some((self.keys.remove(0), self.children.remove(0)))
  }
  pub fn push_back(&mut self, key: Vec<u8>, child: usize) {
    self.keys.push(key);
    self.children.push(child);
  }
  pub fn push_front(&mut self, key: Vec<u8>, child: usize) {
    self.keys.insert(0, key);
    self.children.insert(0, child);
  }

  pub fn merge(&mut self, key: Vec<u8>, node: &mut InternalNode) {
    self.keys.push(key);
    self.keys.append(node.keys.as_mut());
    self.children.append(node.children.as_mut());
  }

  pub fn len(&self) -> usize {
    self.keys.len()
  }

  pub fn next(&self, key: &Vec<u8>) -> usize {
    let i = self
      .keys
      .binary_search_by(|k| k.cmp(key))
      .map(|i| i.add(1))
      .unwrap_or_else(|i| i);
    self.children[i]
  }

  pub fn find_family(
    &self,
    key: &Vec<u8>,
  ) -> ((usize, usize), Option<usize>, Option<usize>) {
    match self.keys.binary_search_by(|k| k.cmp(key)) {
      Ok(i) => (
        (i, self.children[i.add(1)]),
        self.children.get(i).copied(),
        self.children.get(i.add(2)).copied(),
      ),
      Err(i) => (
        (i.sub(1), self.children[i]),
        self.children.get(i.sub(1)).copied(),
        self.children.get(i.add(1)).copied(),
      ),
    }
  }
}

impl Serializable for InternalNode {
  fn serialize(&self) -> Result<Page, Error> {
    let mut p = Page::new();
    let mut wt = p.writer();
    wt.write(&[2])?;
    wt.write(&[self.keys.len() as u8])?;
    for k in &self.keys {
      wt.write(&[k.len() as u8])?;
      wt.write(k.as_ref())?;
    }
    for &i in &self.children {
      wt.write(&i.to_be_bytes())?;
    }
    Ok(p)
  }

  fn deserialize(value: &Page) -> Result<Self, Error> {
    let mut sc = value.scanner();
    sc.read()?;
    let kl = sc.read()?;
    let mut keys = vec![];
    let mut children = vec![];
    for _ in 0..kl {
      let n = sc.read()?;
      keys.push(sc.read_n(n as usize)?.to_vec());
    }
    for _ in 0..(kl + 1) {
      children.push(sc.read_usize()?);
    }

    Ok(InternalNode { keys, children })
  }
}

#[derive(Debug)]
pub struct LeafNode {
  pub keys: Vec<(Vec<u8>, usize)>,
  pub next: Option<usize>,
  pub prev: Option<usize>,
}
impl LeafNode {
  pub fn empty() -> Self {
    Self {
      keys: vec![],
      prev: None,
      next: None,
    }
  }

  pub fn top(&self) -> Vec<u8> {
    self.keys[0].0.clone()
  }

  pub fn split(&mut self, current: usize) -> (CursorEntry, Vec<u8>) {
    let c = self.keys.len().div_ceil(2);
    let keys = self.keys.split_off(c);
    let m = keys[0].0.clone();
    let next = self.next.take();
    (
      CursorEntry::Leaf(LeafNode {
        keys,
        next,
        prev: Some(current),
      }),
      m,
    )
  }

  pub fn set_next(&mut self, next: usize) {
    self.next = Some(next);
  }

  pub fn delete(&mut self, key: &Vec<u8>) -> Option<usize> {
    if let Ok(i) = self.keys.binary_search_by(|(k, _)| k.cmp(key)) {
      return Some(self.keys.remove(i).1);
    }
    None
  }

  pub fn merge(&mut self, node: &mut LeafNode) {
    self.keys.append(node.keys.as_mut());
    self.next = node.next;
  }

  pub fn len(&self) -> usize {
    self.keys.len()
  }

  pub fn find(&self, key: &Vec<u8>) -> Option<usize> {
    self
      .keys
      .binary_search_by(|(k, _)| k.cmp(key))
      .ok()
      .map(|i| self.keys[i].1)
  }

  pub fn pop_front(&mut self) -> Option<(Vec<u8>, usize)> {
    if self.len().eq(&0) {
      return None;
    }
    Some(self.keys.remove(0))
  }
  pub fn pop_back(&mut self) -> Option<(Vec<u8>, usize)> {
    self.keys.pop()
  }
  pub fn push_front(&mut self, key: Vec<u8>, index: usize) {
    self.keys.insert(0, (key, index));
  }
  pub fn push_back(&mut self, key: Vec<u8>, index: usize) {
    self.keys.push((key, index));
  }
}
impl Serializable for LeafNode {
  fn serialize(&self) -> Result<Page, Error> {
    let mut p = Page::new();
    let mut wt = p.writer();
    wt.write(&[1])?;
    wt.write(&[self.keys.len() as u8])?;
    for (k, i) in &self.keys {
      wt.write(&[k.len() as u8])?;
      wt.write(k.as_ref())?;
      wt.write(&i.to_be_bytes())?;
    }
    let prev = self.prev.unwrap_or(0);
    wt.write(&prev.to_be_bytes())?;
    let next = self.next.unwrap_or(0);
    wt.write(&next.to_be_bytes())?;
    Ok(p)
  }

  fn deserialize(value: &Page) -> Result<Self, Error> {
    let mut sc = value.scanner();
    sc.read()?;
    let mut keys = vec![];
    let kl = sc.read()?;
    for _ in 0..kl {
      let n = sc.read()?;
      let k = sc.read_n(n as usize)?.to_vec();
      let i = sc.read_usize()?;
      keys.push((k, i));
    }
    let prev = sc.read_usize()?;
    let prev = if prev.eq(&0) { None } else { Some(prev) };
    let next = sc.read_usize()?;
    let next = if next.eq(&0) { None } else { Some(next) };
    Ok(Self { keys, prev, next })
  }
}
