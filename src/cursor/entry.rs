use std::{collections::VecDeque, mem::replace};

use crate::{
  cursor::Pointer,
  serialize::{Serializable, SerializeType, SERIALIZABLE_BYTES},
  Error,
};

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
}

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
}

pub struct DataEntry {
  next: Option<Pointer>,
  versions: VecDeque<VersionRecord>,
}
impl DataEntry {
  pub fn new() -> Self {
    Self {
      next: None,
      versions: Default::default(),
    }
  }

  pub fn find(&self, version: usize) -> impl Iterator<Item = &VersionRecord> {
    let s = self
      .versions
      .binary_search_by(|v| version.cmp(&v.version))
      .unwrap_or_else(|i| i);
    (s..self.versions.len()).map(|i| &self.versions[i])
  }

  pub fn filter_aborted<F: Fn(&usize) -> bool>(&mut self, is_aborted: F) -> bool {
    let len = self.versions.len();
    self.versions = self
      .versions
      .drain(..)
      .filter(|v| !is_aborted(&v.owner))
      .collect();
    self.versions.len() < len
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

  pub fn apply_split(&mut self, versions: VecDeque<VersionRecord>) {
    let exists = replace(&mut self.versions, versions);
    self.versions.extend(exists);
  }

  pub fn append(&mut self, record: VersionRecord) {
    self.versions.push_front(record);
  }

  pub fn split_if_full(&mut self) -> Option<VecDeque<VersionRecord>> {
    let mut byte_len = 8 + 8;

    for i in 0..self.versions.len() {
      byte_len += 8 * 3 + self.versions[i].data.len();
      if byte_len >= SERIALIZABLE_BYTES {
        return Some(self.versions.split_off(self.versions.len() >> 1));
      }
    }

    None
  }

  pub fn remove_until(&mut self, min_version: usize) -> bool {
    let i = self
      .versions
      .binary_search_by(|v| min_version.cmp(&v.version))
      .unwrap_or_else(|i| i)
      + 1;
    if i >= self.versions.len() {
      return false;
    }
    let _ = self.versions.split_off(i);
    let _ = self.next.take();
    true
  }

  pub fn is_empty(&self) -> bool {
    self.versions.is_empty()
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
        _ => return Err(Error::InvalidFormat),
      };
      versions.push_back(VersionRecord::new(owner, version, data))
    }
    Ok(Self {
      versions,
      next: (next != 0).then_some(next),
    })
  }
}
