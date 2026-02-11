use crate::{Error, Page, Result, Serializable, PAGE_SIZE};

pub type Key = Vec<u8>;
pub type Pointer = usize;

pub enum CursorNode {
  Internal(InternalNode),
  Leaf(LeafNode),
}
impl CursorNode {
  pub fn as_leaf(self) -> Result<LeafNode> {
    match self {
      CursorNode::Internal(_) => Err(Error::InvalidFormat),
      CursorNode::Leaf(node) => Ok(node),
    }
  }
  pub fn as_internal(self) -> Result<InternalNode> {
    match self {
      CursorNode::Internal(node) => Ok(node),
      CursorNode::Leaf(_) => Err(Error::InvalidFormat),
    }
  }
}
impl Serializable for CursorNode {
  fn serialize(&self, page: &mut Page<PAGE_SIZE>) -> Result {
    let mut writer = page.writer();
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

  fn deserialize(page: &Page<PAGE_SIZE>) -> Result<Self> {
    todo!()
  }
}
pub struct InternalNode {
  keys: Vec<Key>,
  children: Vec<Pointer>,
  right: Option<(Pointer, Key)>,
}
impl InternalNode {
  pub fn inialialize(key: Key, left: Pointer, right: Pointer) -> Self {
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
  Found(Pointer),
  Move(Pointer),
  NotFound(usize),
}
pub struct LeafNode {
  entries: Vec<(Key, Pointer)>,
  prev: Option<Pointer>,
  next: Option<Pointer>,
}
impl LeafNode {
  fn new(entries: Vec<(Key, Pointer)>, next: Option<Pointer>) -> Self {
    Self {
      entries,
      prev: Default::default(),
      next,
    }
  }
  pub fn find(&self, key: &Key) -> NodeFindResult {
    match self.entries.binary_search_by(|(k, _)| k.cmp(key)) {
      Ok(i) => NodeFindResult::Found(self.entries[i].1),
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
        ));
      }
    }
    None
  }

  pub fn top(&self) -> &Key {
    &self.entries[0].0
  }
}
