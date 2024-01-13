use crate::{
  disk::{Page, Serializable},
  error::Error,
};

pub static MAX_NODE_LEN: usize = 12;

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
    match value.as_ref()[0] {
      0 => Ok(Self::Leaf(value.deserialize()?)),
      1 => Ok(Self::Internal(value.deserialize()?)),
      _ => Err(Error::Invalid),
    }
  }
}
impl CursorEntry {
  pub fn find_or_next(&self, key: &String) -> Result<usize, Option<usize>> {
    match self {
      Self::Internal(node) => Err(Some(node.next(key))),
      Self::Leaf(node) => node
        .keys
        .binary_search_by(|(k, _)| k.cmp(key))
        .map(|i| node.keys[i].1)
        .map_err(|_| None),
    }
  }
}

pub struct InternalNode {
  pub keys: Vec<String>,
  pub children: Vec<usize>,
}
impl InternalNode {
  pub fn split(&mut self) -> (CursorEntry, String) {
    let c = self.keys.len() / 2;
    let mut keys = self.keys.split_off(c - 1);
    let m = keys.remove(0);
    let children = self.children.split_off(c);
    return (CursorEntry::Internal(InternalNode { keys, children }), m);
  }

  pub fn add(&mut self, key: String, index: usize) {
    if let Err(i) = self.keys.binary_search_by(|k| k.cmp(&key)) {
      let mut keys = self.keys.split_off(i);
      self.keys.push(key);
      self.keys.append(keys.as_mut());
      let mut c = self.children.split_off(i);
      self.children.push(index);
      self.children.append(c.as_mut());
    };
  }

  pub fn len(&self) -> usize {
    self.keys.len()
  }

  pub fn next(&self, key: &String) -> usize {
    let i = self
      .keys
      .binary_search_by(|k| k.cmp(key))
      .unwrap_or_else(|i| i);
    self.children[i]
  }
}

impl Serializable for InternalNode {
  fn serialize(&self) -> Result<Page, Error> {
    let mut p = Page::new();
    let mut wt = p.writer();
    wt.write(&[1])?;
    wt.write(&[self.keys.len() as u8])?;
    for k in &self.keys {
      wt.write(&[k.len() as u8])?;
      wt.write(k.as_bytes())?;
    }
    for i in 0..(self.keys.len() + 1) {
      wt.write(&self.children[i].to_be_bytes())?;
    }
    return Ok(p);
  }
  fn deserialize(value: &Page) -> Result<Self, Error> {
    let mut sc = value.scanner();
    sc.read()?;
    let kl = sc.read().unwrap();
    let mut keys = vec![];
    let mut children = vec![];
    for _ in 0..kl {
      let n = sc.read().unwrap();
      keys.push(String::from_utf8_lossy(sc.read_n(n as usize)?).to_string());
    }
    for _ in 0..(kl + 1) {
      children.push(sc.read_usize()?);
    }

    return Ok(InternalNode { keys, children });
  }
}

pub struct LeafNode {
  pub keys: Vec<(String, usize)>,
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

  pub fn split(
    &mut self,
    current: usize,
    added: usize,
  ) -> (CursorEntry, String) {
    let c = self.keys.len() / 2;
    let keys = self.keys.split_off(c - 1);
    let m = keys[0].0.clone();
    let next = self.next.take();
    self.next = Some(added);
    return (
      CursorEntry::Leaf(LeafNode {
        keys,
        next,
        prev: Some(current),
      }),
      m,
    );
  }

  pub fn add(&mut self, key: String, index: usize) -> Option<String> {
    if let Err(i) = self.keys.binary_search_by(|(k, _)| k.cmp(&key)) {
      let mut keys = self.keys.split_off(i);
      self.keys.push((key, index));
      self.keys.append(keys.as_mut());
    };
    return self.keys.last().map(|(k, _)| k.to_owned());
  }

  pub fn len(&self) -> usize {
    self.keys.len()
  }
}
impl Serializable for LeafNode {
  fn serialize(&self) -> Result<Page, Error> {
    let mut p = Page::new();
    let mut wt = p.writer();
    wt.write(&[0])?;
    wt.write(&[self.keys.len() as u8])?;
    for (k, i) in &self.keys {
      wt.write(&[k.len() as u8])?;
      wt.write(k.as_bytes())?;
      wt.write(&i.to_be_bytes())?;
    }
    let prev = self.prev.unwrap_or(0);
    wt.write(&prev.to_be_bytes())?;
    let next = self.next.unwrap_or(0);
    wt.write(&next.to_be_bytes())?;
    return Ok(p);
  }

  fn deserialize(value: &Page) -> Result<Self, Error> {
    let mut sc = value.scanner();
    sc.read()?;
    let mut keys = vec![];
    let kl = sc.read().unwrap();
    for _ in 0..kl {
      let n = sc.read().unwrap();
      let k = String::from_utf8_lossy(sc.read_n(n as usize)?).to_string();
      let i = sc.read_usize()?;
      keys.push((k, i));
    }
    let prev = sc.read_usize()?;
    let prev = if prev == 0 { None } else { Some(prev) };
    let next = sc.read_usize()?;
    let next = if next == 0 { None } else { Some(next) };
    return Ok(Self { keys, prev, next });
  }
}
