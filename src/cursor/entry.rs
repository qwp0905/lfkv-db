use std::{collections::VecDeque /*  mem::replace */};

use crate::{
  cursor::Pointer,
  serialize::{Serializable, SerializeType, SERIALIZABLE_BYTES},
  Error,
};

#[derive(Debug)]
pub enum RecordData {
  Data(Vec<u8>),
  Tombstone,
}
impl RecordData {
  pub fn len(&self) -> usize {
    match self {
      RecordData::Data(data) => 1 + data.len(),
      RecordData::Tombstone => 1,
    }
  }

  pub fn cloned(&self) -> Option<Vec<u8>> {
    match self {
      RecordData::Data(data) => Some(data.clone()),
      RecordData::Tombstone => None,
    }
  }
}

#[derive(Debug)]
pub struct VersionRecord {
  pub owner: usize,
  pub version: usize,
  pub data: RecordData,
}
impl VersionRecord {
  pub fn new(owner: usize, version: usize, data: RecordData) -> Self {
    Self {
      owner,
      version,
      data,
    }
  }
  fn byte_len(&self) -> usize {
    24 + self.data.len() // owner 8byte + version 8byte + data lenth 8byte + data
  }
}

#[derive(Debug)]
pub struct DataEntry {
  next: Option<Pointer>,
  versions: VecDeque<VersionRecord>,
}
impl DataEntry {
  pub fn init(version: VersionRecord) -> Self {
    let mut versions = VecDeque::new();
    versions.push_front(version);
    Self {
      next: None,
      versions,
    }
  }

  pub fn len(&self) -> usize {
    self.versions.len()
  }
  pub fn take_versions<'a>(&'a mut self) -> impl Iterator<Item = VersionRecord> + 'a {
    self.versions.drain(..)
  }
  pub fn set_versions(&mut self, new_versions: VecDeque<VersionRecord>) {
    self.versions = new_versions;
  }

  pub fn get_versions(&self) -> impl Iterator<Item = &VersionRecord> {
    self.versions.iter()
  }

  pub fn get_last_owner(&self) -> Option<usize> {
    self.versions.front().map(|v| v.owner)
  }

  pub fn get_next(&self) -> Option<Pointer> {
    self.next
  }
  pub fn set_next(&mut self, index: Pointer) {
    self.next = Some(index);
  }

  pub fn append(&mut self, record: VersionRecord) {
    self.versions.push_front(record);
  }

  pub fn is_available(&self, record: &VersionRecord) -> bool {
    let byte_len = 8 + 8 + self.versions.iter().fold(0, |a, c| a + c.byte_len());
    record.byte_len() + byte_len < SERIALIZABLE_BYTES
  }

  pub fn is_empty(&self) -> bool {
    if self.versions.is_empty() {
      return true;
    }
    if self.versions.len() > 1 {
      return false;
    }
    if let RecordData::Tombstone = self.versions[0].data {
      return true;
    }
    false
  }
}
impl Serializable for DataEntry {
  fn get_type() -> SerializeType {
    SerializeType::DataEntry
  }
  fn write_at(&self, writer: &mut crate::disk::PageWriter) -> crate::Result {
    writer.write_usize(self.next.unwrap_or(0))?;
    writer.write_usize(self.versions.len())?;

    for record in &self.versions {
      writer.write_usize(record.version)?;
      writer.write_usize(record.owner)?;
      match &record.data {
        RecordData::Data(data) => {
          writer.write(&[0])?;
          writer.write_usize(data.len())?;
          writer.write(&data)?;
        }
        RecordData::Tombstone => writer.write(&[1])?,
      }
    }
    Ok(())
  }

  fn read_from(reader: &mut crate::disk::PageScanner) -> crate::Result<Self> {
    let next = reader.read_usize()?;
    let len = reader.read_usize()?;
    let mut versions = VecDeque::new();
    for _ in 0..len {
      let version = reader.read_usize()?;
      let owner = reader.read_usize()?;
      let data = match reader.read()? {
        0 => {
          let l = reader.read_usize()?;
          RecordData::Data(reader.read_n(l)?.to_vec())
        }
        1 => RecordData::Tombstone,
        _ => return Err(Error::InvalidFormat("invalid type for data version record")),
      };
      versions.push_back(VersionRecord::new(owner, version, data))
    }
    Ok(Self {
      versions,
      next: (next != 0).then_some(next),
    })
  }
}

#[cfg(test)]
#[path = "tests/entry.rs"]
mod tests;
