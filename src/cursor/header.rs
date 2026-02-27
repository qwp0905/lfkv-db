use crate::{
  disk::{PageScanner, PageWriter},
  serialize::{Serializable, SerializeType},
  Result,
};

pub const HEADER_INDEX: usize = 0;

#[derive(Debug)]
pub struct TreeHeader {
  root: usize,
}

impl TreeHeader {
  pub fn new(root: usize) -> Self {
    Self { root }
  }

  pub fn get_root(&self) -> usize {
    self.root
  }

  pub fn set_root(&mut self, index: usize) {
    self.root = index
  }
}

impl Serializable for TreeHeader {
  fn get_type() -> SerializeType {
    SerializeType::DataEntry
  }

  fn write_at(&self, writer: &mut PageWriter) -> Result {
    writer.write_usize(self.root)
  }

  fn read_from(reader: &mut PageScanner) -> Result<Self> {
    let root = reader.read_usize()?;
    Ok(Self { root })
  }
}

#[cfg(test)]
mod tests {
  use crate::{disk::Page, serialize::SerializeFrom};

  use super::*;

  #[test]
  fn test_tree_header_roundtrip() {
    let mut page = Page::new();
    let header = TreeHeader::new(42);
    page.serialize_from(&header).expect("serialize error");

    let decoded: TreeHeader = page.deserialize().expect("deserialize error");
    assert_eq!(decoded.get_root(), 42);
  }

  #[test]
  fn test_tree_header_zero_root() {
    let mut page = Page::new();
    let header = TreeHeader::new(0);
    page.serialize_from(&header).expect("serialize error");

    let decoded: TreeHeader = page.deserialize().expect("deserialize error");
    assert_eq!(decoded.get_root(), 0);
  }

  #[test]
  fn test_tree_header_large_root() {
    let mut page = Page::new();
    let header = TreeHeader::new(usize::MAX);
    page.serialize_from(&header).expect("serialize error");

    let decoded: TreeHeader = page.deserialize().expect("deserialize error");
    assert_eq!(decoded.get_root(), usize::MAX);
  }
}
