use std::mem::replace;

use crate::{cursor::Pointer, Error, Page, Serializable, PAGE_SIZE};

pub type DataVersion = (usize, Vec<u8>);

pub struct DataEntrySlot<'a> {
  versions: &'a mut Vec<DataVersion>,
  i: usize,
  split: bool,
  tx_id: usize,
}
impl<'a> DataEntrySlot<'a> {
  fn new(
    versions: &'a mut Vec<DataVersion>,
    i: usize,
    split: bool,
    tx_id: usize,
  ) -> Self {
    Self {
      versions,
      i,
      split,
      tx_id,
    }
  }

  pub fn alloc(self, data: Vec<u8>) {
    if !self.split {
      let _ = replace(&mut self.versions[self.i].1, data);
      return;
    }

    let tmp = self.versions.split_off(self.i);
    self.versions.push((self.tx_id, data));
    self.versions.extend(tmp);
  }
}
pub struct DataEntry {
  next: Option<Pointer>,
  versions: Vec<DataVersion>,
}
impl DataEntry {
  pub fn new() -> Self {
    Self {
      next: None,
      versions: Default::default(),
    }
  }
  pub fn find(
    &self,
    tx_id: usize,
  ) -> std::result::Result<&Vec<u8>, impl Iterator<Item = &DataVersion>> {
    let s = match self
      .versions
      .binary_search_by(|(version, _)| tx_id.cmp(version))
    {
      Ok(i) => return Ok(&self.versions[i].1),
      Err(i) => i,
    };
    Err((s..self.versions.len()).map(|i| &self.versions[i]))
  }

  pub fn find_slot(
    &mut self,
    tx_id: usize,
  ) -> std::result::Result<DataEntrySlot<'_>, usize> {
    match self
      .versions
      .binary_search_by(|(version, _)| tx_id.cmp(version))
    {
      Ok(i) => Ok(DataEntrySlot::new(&mut self.versions, i, false, tx_id)),
      Err(i) => {
        if i == self.versions.len() {
          if let Some(next) = self.next {
            return Err(next);
          }
        }

        Ok(DataEntrySlot::new(&mut self.versions, i, true, tx_id))
      }
    }
  }

  pub fn get_next(&self) -> Option<Pointer> {
    self.next
  }
  pub fn set_next(&mut self, index: Pointer) {
    self.next = Some(index);
  }

  pub fn apply_split(&mut self, versions: Vec<DataVersion>) {
    let exists = replace(&mut self.versions, versions);
    self.versions.extend(exists);
  }

  pub fn split_if_full(&mut self) -> Option<Vec<DataVersion>> {
    let mut byte_len = 8 + 8;

    for i in 0..self.versions.len() {
      byte_len += 8 + 8 + self.versions[i].1.len();
      if byte_len >= PAGE_SIZE {
        return Some(self.versions.split_off(self.versions.len() >> 1));
      }
    }

    None
  }
}
impl Serializable for DataEntry {
  fn serialize(&self, page: &mut Page<PAGE_SIZE>) -> std::result::Result<(), Error> {
    let mut writer = page.writer();
    writer.write_usize(self.next.unwrap_or(0))?;
    writer.write_usize(self.versions.len())?;

    for (id, data) in &self.versions {
      writer.write_usize(*id)?;
      writer.write_usize(data.len())?;
      writer.write(data.as_ref())?;
    }
    Ok(())
  }

  fn deserialize(page: &Page<PAGE_SIZE>) -> std::result::Result<Self, Error> {
    let mut reader = page.scanner();
    let next = reader.read_usize()?;
    let len = reader.read_usize()?;
    let mut versions = Vec::new();
    for _ in 0..len {
      let id = reader.read_usize()?;
      let l = reader.read_usize()?;
      let data = reader.read_n(l)?.to_vec();
      versions.push((id, data))
    }
    Ok(Self {
      versions,
      next: (next != 0).then_some(next),
    })
  }
}
