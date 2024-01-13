use crate::{
  disk::{Page, Serializable},
  error::{Error, Result},
};

use super::Node;

pub struct CursorEntry {
  pub index: usize,
  pub node: Node,
}
impl CursorEntry {
  fn new(index: usize, node: Node) -> Self {
    Self { index, node }
  }

  pub fn from(index: usize, page: Page) -> Result<Self> {
    Ok(Self::new(index, page.deserialize()?))
  }

  pub fn find_next(
    &self,
    key: &String,
  ) -> core::result::Result<usize, Option<usize>> {
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
}
impl Serializable for CursorEntry {
  fn serialize(&self) -> std::prelude::v1::Result<Page, Error> {
    self.node.serialize()
  }

  fn deserialize(_: &Page) -> std::prelude::v1::Result<Self, Error> {
    Err(Error::Invalid)
  }
}
