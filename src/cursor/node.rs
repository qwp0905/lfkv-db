use super::{Key, Pointer};
use crate::{
  disk::{PageScanner, PageWriter, PAGE_SIZE},
  serialize::{Serializable, SerializeType},
  Error, Result,
};

#[derive(Debug)]
pub enum CursorNode {
  Internal(InternalNode),
  Leaf(LeafNode),
}
impl CursorNode {
  pub fn initial_state() -> Self {
    Self::Leaf(LeafNode::new(Default::default(), None, None))
  }
  pub fn as_leaf(self) -> Result<LeafNode> {
    match self {
      CursorNode::Internal(_) => Err(Error::InvalidFormat("invalid leaf node type")),
      CursorNode::Leaf(node) => Ok(node),
    }
  }
  pub fn as_internal(self) -> Result<InternalNode> {
    match self {
      CursorNode::Internal(node) => Ok(node),
      CursorNode::Leaf(_) => Err(Error::InvalidFormat("invalid internal node type")),
    }
  }
}
impl Serializable for CursorNode {
  fn get_type() -> SerializeType {
    SerializeType::CursorNode
  }
  fn write_at(&self, writer: &mut PageWriter) -> Result {
    match self {
      CursorNode::Internal(node) => {
        writer.write(&[0])?;
        match &node.right {
          Some((pointer, key)) => {
            writer.write(&[1])?;
            writer.write_usize(*pointer)?;
            writer.write_usize(key.len())?;
            writer.write(key)
          }
          None => writer.write(&[0]),
        }?;
        writer.write_usize(node.keys.len())?;
        for key in &node.keys {
          writer.write_usize(key.len())?;
          writer.write(key)?;
        }
        for ptr in &node.children {
          writer.write_usize(*ptr)?;
        }
      }
      CursorNode::Leaf(node) => {
        writer.write(&[1])?;
        writer.write_usize(node.prev.unwrap_or(0))?;
        writer.write_usize(node.next.unwrap_or(0))?;
        writer.write_usize(node.entries.len())?;
        for (key, pointer) in &node.entries {
          writer.write_usize(key.len())?;
          writer.write(&key)?;
          writer.write_usize(*pointer)?;
        }
      }
    }
    Ok(())
  }

  fn read_from(scanner: &mut PageScanner) -> Result<Self> {
    match scanner.read()? {
      0 => {
        //internal
        let mut right = None;
        if scanner.read()? == 1 {
          let ptr = scanner.read_usize()?;
          let len = scanner.read_usize()?;
          let key = scanner.read_n(len)?.to_vec();
          right = Some((ptr, key));
        };

        let len = scanner.read_usize()?;
        let mut keys = Vec::new();
        for _ in 0..len {
          let l = scanner.read_usize()?;
          keys.push(scanner.read_n(l)?.to_vec());
        }

        let mut children = Vec::new();
        for _ in 0..=len {
          children.push(scanner.read_usize()?);
        }
        Ok(Self::Internal(InternalNode::new(keys, children, right)))
      }
      1 => {
        // leaf
        let prev = scanner.read_usize()?;
        let next = scanner.read_usize()?;
        let len = scanner.read_usize()?;
        let mut entries = Vec::new();
        for _ in 0..len {
          let l = scanner.read_usize()?;
          let key = scanner.read_n(l)?.to_vec();
          let ptr = scanner.read_usize()?;
          entries.push((key, ptr));
        }
        Ok(Self::Leaf(LeafNode::new(
          entries,
          (next != 0).then(|| next),
          (prev != 0).then(|| prev),
        )))
      }
      _ => Err(Error::InvalidFormat("invalid cursor node type")),
    }
  }
}
#[derive(Debug)]
pub struct InternalNode {
  keys: Vec<Key>,
  children: Vec<Pointer>,
  right: Option<(Pointer, Key)>,
}
impl InternalNode {
  pub fn initialize(key: Key, left: Pointer, right: Pointer) -> Self {
    Self::new(vec![key], vec![left, right], None)
  }
  fn new(keys: Vec<Key>, children: Vec<Pointer>, right: Option<(Pointer, Key)>) -> Self {
    Self {
      keys,
      children,
      right,
    }
  }
  pub fn find(&self, key: &Key) -> std::result::Result<Pointer, Pointer> {
    if let Some((right, high)) = &self.right {
      if high <= key {
        return Err(*right);
      }
    };
    match self.keys.binary_search_by(|k| k.cmp(key)) {
      Ok(i) => Ok(self.children[i + 1]),
      Err(i) => Ok(self.children[i]),
    }
  }
  pub fn first_child(&self) -> Pointer {
    self.children[0]
  }
  pub fn insert_or_next(
    &mut self,
    key: &Key,
    pointer: Pointer,
  ) -> std::result::Result<(), Pointer> {
    if let Some((right, high)) = &self.right {
      if high <= key {
        return Err(*right);
      }
    };
    let at = self
      .keys
      .binary_search_by(|k| k.cmp(key))
      .unwrap_or_else(|i| i);

    let tmp = self.keys.split_off(at);
    self.keys.push(key.clone());
    self.keys.extend(tmp);
    let tmp = self.children.split_off(at + 1);
    self.children.push(pointer);
    self.children.extend(tmp);
    Ok(())
  }

  pub fn split_if_needed(&mut self) -> Option<(InternalNode, Key)> {
    let mut byte_len = 1 + 1 + 8 + self.children.len() * 8;
    for i in 0..self.keys.len() {
      byte_len += 8 * 2 + self.keys[i].len();
      if byte_len >= PAGE_SIZE {
        let mid = self.keys.len() >> 1;
        let keys = self.keys.split_off(mid + 1);
        let mid_key = self.keys.pop().unwrap();
        let children = self.children.split_off(mid + 1);

        return Some((
          InternalNode::new(keys, children, self.right.take()),
          mid_key,
        ));
      }
    }
    None
  }

  pub fn set_right(&mut self, key: &Key, ptr: Pointer) -> Option<(Pointer, Key)> {
    self.right.replace((ptr, key.clone()))
  }
}

pub enum NodeFindResult {
  Found(usize, Pointer),
  Move(Pointer),
  NotFound(usize),
}
#[derive(Debug)]
pub struct LeafNode {
  entries: Vec<(Key, Pointer)>,
  prev: Option<Pointer>,
  next: Option<Pointer>,
}
impl LeafNode {
  fn new(
    entries: Vec<(Key, Pointer)>,
    next: Option<Pointer>,
    prev: Option<Pointer>,
  ) -> Self {
    Self {
      entries,
      prev,
      next,
    }
  }
  pub fn find(&self, key: &Key) -> NodeFindResult {
    match self.entries.binary_search_by(|(k, _)| k.cmp(key)) {
      Ok(i) => NodeFindResult::Found(i, self.entries[i].1),
      Err(i) => {
        if i == self.entries.len() {
          if let Some(p) = self.next {
            return NodeFindResult::Move(p);
          }
        };

        NodeFindResult::NotFound(i)
      }
    }
  }
  pub fn at(&self, i: usize) -> &(Key, Pointer) {
    &self.entries[i]
  }
  pub fn len(&self) -> usize {
    self.entries.len()
  }

  pub fn drain(&mut self) -> impl Iterator<Item = (Key, Pointer)> + use<'_> {
    self.entries.drain(..)
  }

  pub fn get_entries(&self) -> impl Iterator<Item = &(Key, Pointer)> {
    self.entries.iter()
  }
  pub fn set_entries(&mut self, entries: Vec<(Key, Pointer)>) {
    self.entries = entries;
  }

  pub fn get_next(&self) -> Option<Pointer> {
    self.next
  }
  pub fn set_next(&mut self, pointer: Pointer) -> Option<Pointer> {
    self.next.replace(pointer)
  }
  pub fn set_prev(&mut self, pointer: Pointer) -> Option<Pointer> {
    self.prev.replace(pointer)
  }

  pub fn insert_at(
    &mut self,
    index: usize,
    key: Key,
    pointer: Pointer,
  ) -> Option<LeafNode> {
    let tmp = self.entries.split_off(index);
    self.entries.push((key, pointer));
    self.entries.extend(tmp);

    let mut byte_len = 1 + 8 + 8 + 8;
    for i in 0..self.entries.len() {
      byte_len += 8 * 2 + self.entries[i].0.len();
      if byte_len >= PAGE_SIZE {
        return Some(LeafNode::new(
          self.entries.split_off(self.entries.len() >> 1),
          self.next.take(),
          None,
        ));
      }
    }
    None
  }

  pub fn top(&self) -> &Key {
    &self.entries[0].0
  }
}

#[cfg(test)]
mod tests {
  use crate::{disk::Page, serialize::SerializeFrom};

  use super::*;

  #[test]
  fn test_serialize_internal() {
    let mut page = Page::new();
    let node = CursorNode::Internal(InternalNode::new(vec![], vec![10], None));
    page.serialize_from(&node).expect("serialize error");

    let d = page
      .deserialize::<CursorNode>()
      .expect("desiralize error")
      .as_internal()
      .expect("desirialize internal error");

    assert_eq!(d.keys.len(), 0);
    assert_eq!(d.children.len(), 1);
    assert_eq!(d.children[0], 10);
    assert_eq!(d.right, None)
  }

  #[test]
  fn test_serialize_leaf() {
    let mut page = Page::new();

    let key = vec![49, 50, 51];
    let ptr = 100;
    let prev = None;
    let next = Some(1100);

    let node = CursorNode::Leaf(LeafNode::new(vec![(key.clone(), ptr)], next, prev));
    page.serialize_from(&node).expect("serialize error");

    let d = page
      .deserialize::<CursorNode>()
      .expect("desiralize error")
      .as_leaf()
      .expect("desirialize leaf error");
    assert_eq!(d.entries, vec![(key, ptr)]);
    assert_eq!(d.next, next);
    assert_eq!(d.prev, prev);
  }

  #[test]
  fn test_serialize_internal_with_keys_and_right() {
    let mut page = Page::new();
    let node = CursorNode::Internal(InternalNode::new(
      vec![vec![1, 2], vec![3, 4]],
      vec![10, 20, 30],
      Some((99, vec![5, 6])),
    ));
    page.serialize_from(&node).expect("serialize error");

    let d = page
      .deserialize::<CursorNode>()
      .expect("deserialize error")
      .as_internal()
      .expect("as_internal error");

    assert_eq!(d.keys, vec![vec![1, 2], vec![3, 4]]);
    assert_eq!(d.children, vec![10, 20, 30]);
    assert_eq!(d.right, Some((99, vec![5, 6])));
  }

  #[test]
  fn test_serialize_leaf_with_prev_and_next() {
    let mut page = Page::new();
    let node = CursorNode::Leaf(LeafNode::new(
      vec![(vec![1], 10), (vec![2], 20)],
      Some(50),
      Some(40),
    ));
    page.serialize_from(&node).expect("serialize error");

    let d = page
      .deserialize::<CursorNode>()
      .expect("deserialize error")
      .as_leaf()
      .expect("as_leaf error");

    assert_eq!(d.entries, vec![(vec![1], 10), (vec![2], 20)]);
    assert_eq!(d.prev, Some(40));
    assert_eq!(d.next, Some(50));
  }

  #[test]
  fn test_serialize_empty_leaf() {
    let mut page = Page::new();
    let node = CursorNode::Leaf(LeafNode::new(vec![], None, None));
    page.serialize_from(&node).expect("serialize error");

    let d = page
      .deserialize::<CursorNode>()
      .expect("deserialize error")
      .as_leaf()
      .expect("as_leaf error");

    assert!(d.entries.is_empty());
    assert_eq!(d.prev, None);
    assert_eq!(d.next, None);
  }
}
