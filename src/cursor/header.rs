use std::ops::Add;

use crate::{
  disk::{Page, Serializable},
  error::Error,
  PAGE_SIZE,
};

pub const HEADER_INDEX: usize = 0;

#[derive(Debug)]
pub struct TreeHeader {
  root: usize,
}

impl TreeHeader {
  pub fn initial_state() -> Self {
    Self {
      root: HEADER_INDEX.add(1),
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
  fn serialize(&self, page: &mut Page<PAGE_SIZE>) -> Result<(), Error> {
    page.writer().write_usize(self.root)
  }

  fn deserialize(page: &Page<PAGE_SIZE>) -> Result<Self, Error> {
    let root = page.scanner().read_usize()?;
    Ok(Self { root })
  }
}
