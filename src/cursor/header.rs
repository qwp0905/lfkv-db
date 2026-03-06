use crate::{
  disk::{PageScanner, PageWriter},
  serialize::{Serializable, SerializeType},
  Result,
};

pub const HEADER_INDEX: usize = 0;

#[derive(Debug)]
pub struct TreeHeader {
  root: usize,
  height: u32,
}

impl TreeHeader {
  pub fn new(root: usize) -> Self {
    Self { root, height: 0 }
  }

  pub fn get_root(&self) -> usize {
    self.root
  }

  pub fn set_root(&mut self, index: usize) {
    self.root = index
  }
  pub fn increase_height(&mut self) {
    self.height += 1;
  }
  pub fn get_height(&self) -> u32 {
    self.height
  }
}

impl Serializable for TreeHeader {
  fn get_type() -> SerializeType {
    SerializeType::Header
  }

  fn write_at(&self, writer: &mut PageWriter) -> Result {
    writer.write_usize(self.root)?;
    writer.write_u32(self.height)?;
    Ok(())
  }

  fn read_from(reader: &mut PageScanner) -> Result<Self> {
    let root = reader.read_usize()?;
    let height = reader.read_u32()?;
    Ok(Self { root, height })
  }
}

#[cfg(test)]
mod tests {
  use crate::{disk::Page, serialize::SerializeFrom};

  use super::*;

  #[test]
  fn test_tree_header_roundtrip() {
    let mut page = Page::new();
    let height = 0u32;
    let root = 42usize;
    let mut header = TreeHeader::new(root);
    header.height = height;
    page.serialize_from(&header).expect("serialize error");

    let decoded: TreeHeader = page.deserialize().expect("deserialize error");
    assert_eq!(decoded.get_root(), root);
    assert_eq!(decoded.get_height(), height);
  }

  #[test]
  fn test_tree_header_zero_root() {
    let mut page = Page::new();
    let height = 123u32;
    let root = 0usize;
    let mut header = TreeHeader::new(root);
    header.height = height;
    page.serialize_from(&header).expect("serialize error");

    let decoded: TreeHeader = page.deserialize().expect("deserialize error");
    assert_eq!(decoded.get_root(), root);
    assert_eq!(decoded.get_height(), height);
  }

  #[test]
  fn test_tree_header_large_root() {
    let mut page = Page::new();
    let height = u32::MAX;
    let root = usize::MAX;
    let mut header = TreeHeader::new(root);
    header.height = height;
    page.serialize_from(&header).expect("serialize error");

    let decoded: TreeHeader = page.deserialize().expect("deserialize error");
    assert_eq!(decoded.get_root(), root);
    assert_eq!(decoded.get_height(), height);
  }
}
