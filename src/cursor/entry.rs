use super::node::Node;

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
      Node::Internal(node) => {
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
      Node::Internal(node) => {
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
