use std::collections::BTreeSet;

use crate::{
  disk::{Page, Serializable},
  error::Error,
};

pub static HEADER_INDEX: usize = 0;

pub struct TreeHeader {
  root: usize,
  last_index: usize,
  fragments: BTreeSet<usize>,
}

impl TreeHeader {
  pub fn acquire_index(&mut self) -> usize {
    if let Some(i) = self.fragments.pop_first() {
      return i;
    };

    self.last_index += 1;
    return self.last_index;
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
    wt.write(&self.last_index.to_be_bytes())?;
    wt.write(&self.fragments.len().to_be_bytes())?;
    for f in &self.fragments {
      wt.write(&f.to_be_bytes()).unwrap();
    }
    return Ok(p);
  }

  fn deserialize(value: &Page) -> Result<Self, Error> {
    let mut s = value.scanner();
    let root = s.read_usize()?;
    let last_index = s.read_usize()?;
    let mut fragments = BTreeSet::new();
    let len = s.read_usize()?;
    for _ in 0..len {
      fragments.insert(s.read_usize()?);
    }

    Ok(TreeHeader {
      root,
      last_index,
      fragments,
    })
  }
}
