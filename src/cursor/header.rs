use std::ops::Add;

use crate::{
  disk::{Page, Serializable},
  error::Error,
};

pub static HEADER_INDEX: usize = 0;

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
  fn serialize(&self) -> Result<Page, Error> {
    let mut p = Page::new();
    let mut wt = p.writer();
    wt.write(&self.root.to_be_bytes())?;
    Ok(p)
  }

  fn deserialize(value: &Page) -> Result<Self, Error> {
    let mut s = value.scanner();
    let root = s.read_usize()?;

    Ok(TreeHeader { root })
  }
}
