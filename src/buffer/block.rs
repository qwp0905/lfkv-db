use crate::{size, Error, Page, Serializable, PAGE_SIZE};

pub const BLOCK_SIZE: usize = size::kb(4);

pub struct DataBlock {
  pub commit_index: usize,
  pub tx_id: usize,
  pub undo_index: usize,
  pub data: Page,
}

impl DataBlock {
  pub fn uncommitted(tx_id: usize, undo_index: usize, data: Page) -> Self {
    Self::new(0, tx_id, undo_index, data)
  }

  pub fn new(commit_index: usize, tx_id: usize, undo_index: usize, data: Page) -> Self {
    Self {
      commit_index,
      tx_id,
      undo_index,
      data,
    }
  }

  pub fn copy(&self) -> Self {
    Self::new(
      self.commit_index,
      self.tx_id,
      self.undo_index,
      self.data.copy(),
    )
  }
}
impl Serializable<Error, BLOCK_SIZE> for DataBlock {
  fn serialize(&self) -> std::prelude::v1::Result<Page<BLOCK_SIZE>, Error> {
    let mut page = Page::new();
    let mut wt = page.writer();
    wt.write(self.commit_index.to_be_bytes().as_ref())?;
    wt.write(self.tx_id.to_be_bytes().as_ref())?;
    wt.write(self.undo_index.to_be_bytes().as_ref())?;
    wt.write(self.data.as_ref())?;
    Ok(page)
  }
  fn deserialize(value: &Page<BLOCK_SIZE>) -> std::prelude::v1::Result<Self, Error> {
    let mut sc = value.scanner();
    let commit_index = sc.read_usize()?;
    let tx_id = sc.read_usize()?;
    let undo_index = sc.read_usize()?;
    let data = sc.read_n(PAGE_SIZE)?.into();
    Ok(Self::new(commit_index, tx_id, undo_index, data))
  }
}
