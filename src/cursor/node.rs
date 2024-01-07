pub enum Node {
  Interior(InteriorNode),
  Leaf(LeafNode),
}

pub struct InteriorNode {
  keys: Vec<String>,
  children: Vec<usize>,
}
impl InteriorNode {
  fn split(&mut self) -> (Node, String) {
    let c = self.keys.len() / 2;
    let mut keys = self.keys.split_off(c - 1);
    let m = keys.remove(0);
    let children = self.children.split_off(c);
    return (Node::Interior(InteriorNode { keys, children }), m);
  }
}

pub struct LeafNode {
  keys: Vec<(String, usize)>,
  next: Option<usize>,
  prev: Option<usize>,
}
impl LeafNode {
  fn split(&mut self, current: usize, added: usize) -> (Node, String) {
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
}

pub struct CursorEntry {
  index: usize,
  node: Node,
}
impl CursorEntry {
  fn new(index: usize, node: Node) -> Self {
    Self { index, node }
  }

  pub fn find_next(&self, key: &String) -> Result<usize, Option<usize>> {
    match &self.node {
      Node::Interior(node) => {
        let i = node
          .keys
          .binary_search_by(|k| k.cmp(key))
          .unwrap_or_else(|i| i);
        return Err(Some(node.children[i]));
      }
      Node::Leaf(node) => node
        .keys
        .binary_search_by(|(k, _)| k.cmp(key))
        .map(|i| node.keys[i].1)
        .map_err(|_| None),
    }
  }

  pub fn split(&mut self, added: usize) -> (Self, String) {
    match &mut self.node {
      Node::Interior(node) => {
        let (n, s) = node.split();
        return (Self::new(added, n), s);
      }
      Node::Leaf(node) => {
        let (n, s) = node.split(self.index, added);
        return (Self::new(added, n), s);
      }
    }
  }
}
