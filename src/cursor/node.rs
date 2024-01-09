use crate::{disk::Page, error::ErrorKind};

pub enum Node {
  Internal(InternalNode),
  Leaf(LeafNode),
}
impl TryFrom<Page> for Node {
  type Error = ErrorKind;
  fn try_from(value: Page) -> Result<Self, Self::Error> {
    match value.as_ref()[0] {
      0 => Ok(Self::Leaf(LeafNode::try_from(value)?)),
      1 => Ok(Self::Internal(InternalNode::try_from(value)?)),
      _ => Err(ErrorKind::Invalid),
    }
  }
}
impl TryFrom<Node> for Page {
  type Error = ErrorKind;
  fn try_from(value: Node) -> Result<Self, Self::Error> {
    value.try_into()
  }
}

pub struct InternalNode {
  pub keys: Vec<String>,
  pub children: Vec<usize>,
}
impl InternalNode {
  pub fn split(&mut self) -> (Node, String) {
    let c = self.keys.len() / 2;
    let mut keys = self.keys.split_off(c - 1);
    let m = keys.remove(0);
    let children = self.children.split_off(c);
    return (Node::Internal(InternalNode { keys, children }), m);
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
}
impl TryFrom<InternalNode> for Page {
  type Error = ErrorKind;
  fn try_from(value: InternalNode) -> Result<Self, Self::Error> {
    let mut p = Page::new();
    let mut wt = p.writer();
    wt.write(&[1])?;
    wt.write(&[value.keys.len() as u8])?;
    for k in &value.keys {
      wt.write(&[k.len() as u8])?;
      wt.write(k.as_bytes())?;
    }
    for i in 0..(value.keys.len() + 1) {
      wt.write(&value.children[i].to_be_bytes())?;
    }
    return Ok(p);
  }
}
impl TryFrom<Page> for InternalNode {
  type Error = ErrorKind;
  fn try_from(value: Page) -> Result<Self, Self::Error> {
    let mut sc = value.scanner();
    sc.read();
    let kl = sc.read().unwrap();
    let mut keys = vec![];
    let mut children = vec![];
    for _ in 0..kl {
      let n = sc.read().unwrap();
      keys.push(String::from_utf8_lossy(sc.read_n(n as usize)).to_string());
    }
    for _ in 0..(kl + 1) {
      children.push(sc.read_usize());
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
  pub fn split(&mut self, current: usize, added: usize) -> (Node, String) {
    let c = self.keys.len() / 2;
    let keys = self.keys.split_off(c - 1);
    let m = keys[0].0.clone();
    let next = self.next.take();
    self.next = Some(added);
    return (
      Node::Leaf(LeafNode {
        keys,
        next,
        prev: Some(current),
      }),
      m,
    );
  }

  pub fn add(&mut self, key: String, index: usize) -> Option<String> {
    if let Err(i) = self.keys.binary_search_by(|k| k.0.cmp(&key)) {
      let mut keys = self.keys.split_off(i);
      self.keys.push((key.to_owned(), index));
      self.keys.append(keys.as_mut());
      return i.eq(&0).then(|| key);
    };
    return None;
  }
}

impl TryFrom<LeafNode> for Page {
  type Error = ErrorKind;
  fn try_from(value: LeafNode) -> Result<Self, Self::Error> {
    let mut p = Page::new();
    let mut wt = p.writer();
    wt.write(&[0])?;
    wt.write(&[value.keys.len() as u8])?;
    for (k, i) in &value.keys {
      wt.write(&[k.len() as u8])?;
      wt.write(k.as_bytes())?;
      wt.write(&i.to_be_bytes())?;
    }
    let prev = value.prev.unwrap_or(0);
    wt.write(&prev.to_be_bytes())?;
    let next = value.next.unwrap_or(0);
    wt.write(&next.to_be_bytes())?;
    return Ok(p);
  }
}

impl TryFrom<Page> for LeafNode {
  type Error = ErrorKind;
  fn try_from(value: Page) -> Result<Self, Self::Error> {
    let mut sc = value.scanner();
    sc.read();
    let mut keys = vec![];
    let kl = sc.read().unwrap();
    for _ in 0..kl {
      let n = sc.read().unwrap();
      let k = String::from_utf8_lossy(sc.read_n(n as usize)).to_string();
      let i = sc.read_usize();
      keys.push((k, i));
    }
    let prev = sc.read_usize();
    let prev = if prev == 0 { None } else { Some(prev) };
    let next = sc.read_usize();
    let next = if next == 0 { None } else { Some(next) };
    return Ok(Self { keys, prev, next });
  }
}
