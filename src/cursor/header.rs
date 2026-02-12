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
  pub fn initial_state() -> Self {
    Self {
      root: HEADER_INDEX + 1,
    }
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
