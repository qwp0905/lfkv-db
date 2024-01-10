use std::collections::VecDeque;

use crate::{disk::Page, error::ErrorKind};

pub struct TreeHeader {
  root: usize,
  last_index: usize,
  fragments: VecDeque<usize>,
}

impl TreeHeader {
  pub fn acquire_index(&mut self) -> usize {
    if let Some(i) = self.fragments.pop_front() {
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

impl TryFrom<TreeHeader> for Page {
  type Error = ErrorKind;
  fn try_from(value: TreeHeader) -> Result<Self, Self::Error> {
    let mut p = Page::new();
    let mut wt = p.writer();
    wt.write(&value.root.to_be_bytes())?;
    wt.write(&value.last_index.to_be_bytes())?;
    wt.write(&value.fragments.len().to_be_bytes())?;
    for f in value.fragments {
      wt.write(&f.to_be_bytes()).unwrap();
    }
    return Ok(p);
  }
}

impl TryFrom<Page> for TreeHeader {
  type Error = ErrorKind;
  fn try_from(value: Page) -> Result<Self, Self::Error> {
    let mut s = value.scanner();
    let root = s.read_usize()?;
    let last_index = s.read_usize()?;
    let mut fragments = VecDeque::new();
    let len = s.read_usize()?;
    for _ in 0..len {
      fragments.push_back(s.read_usize()?);
    }

    Ok(TreeHeader {
      root,
      last_index,
      fragments,
    })
  }
}
