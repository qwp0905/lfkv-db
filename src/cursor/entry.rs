use crate::{
  disk::{Page, Serializable},
  error::Error,
};

pub static MAX_NODE_LEN: usize = 12;

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
      0 => Ok(Self::Leaf(value.deserialize()?)),
      1 => Ok(Self::Internal(value.deserialize()?)),
      _ => Err(Error::Invalid),
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
}

#[derive(Debug)]
pub struct InternalNode {
  pub keys: Vec<Vec<u8>>,
  pub children: Vec<usize>,
}
impl InternalNode {
  pub fn split(&mut self) -> (CursorEntry, Vec<u8>) {
    let c = self.keys.len() / 2;
    let mut keys = self.keys.split_off(c);
    let m = keys.remove(0);
    let children = self.children.split_off(c + 1);
    (CursorEntry::Internal(InternalNode { keys, children }), m)
  }

  pub fn add(&mut self, key: Vec<u8>, index: usize) {
    if let Err(i) = self.keys.binary_search_by(|k| k.cmp(&key)) {
      let mut keys = self.keys.split_off(i);
      self.keys.push(key);
      self.keys.append(keys.as_mut());
      let mut c = self.children.split_off(i + 1);
      self.children.push(index);
      self.children.append(c.as_mut());
    };
  }

  pub fn len(&self) -> usize {
    self.keys.len()
  }

  pub fn next(&self, key: &Vec<u8>) -> usize {
    let i = self
      .keys
      .binary_search_by(|k| k.cmp(key))
      .map(|i| i + 1)
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
  // pub fn empty() -> Self {
  //   Self {
  //     keys: vec![],
  //     prev: None,
  //     next: None,
  //   }
  // }

  pub fn split(&mut self, current: usize, added: usize) -> (CursorEntry, Vec<u8>) {
    let c = self.keys.len() / 2;
    let keys = self.keys.split_off(c);
    let m = keys[0].0.clone();
    let next = self.next.take();
    self.next = Some(added);
    (
      CursorEntry::Leaf(LeafNode {
        keys,
        next,
        prev: Some(current),
      }),
      m,
    )
  }

  pub fn add(&mut self, key: Vec<u8>, index: usize) -> Option<Vec<u8>> {
    if let Err(i) = self.keys.binary_search_by(|(k, _)| k.cmp(&key)) {
      let mut keys = self.keys.split_off(i);
      self.keys.push((key.to_owned(), index));
      self.keys.append(keys.as_mut());
      return i.eq(&0).then(|| key);
    };
    None
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
}
impl Serializable for LeafNode {
  fn serialize(&self) -> Result<Page, Error> {
    let mut p = Page::new();
    let mut wt = p.writer();
    wt.write(&[0])?;
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
    let prev = if prev == 0 { None } else { Some(prev) };
    let next = sc.read_usize()?;
    let next = if next == 0 { None } else { Some(next) };
    Ok(Self { keys, prev, next })
  }
}
