pub enum Node {
  Internal(InternalNode),
  Leaf(LeafNode),
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
}
