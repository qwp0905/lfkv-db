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

  pub fn add(&mut self, key: String, index: usize) -> Option<String> {
    match &mut self.node {
      Node::Internal(node) => {
        node.add(key, index);
        return None;
      }
      Node::Leaf(node) => return node.add(key, index),
    }
  }

  pub fn len(&self) -> usize {
    self.node.len()
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
