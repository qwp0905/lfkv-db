use std::{collections::VecDeque, mem::replace};

use crate::{cursor::Pointer, Error, Page, Serializable, PAGE_SIZE};

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

  fn deserialize(page: &Page<PAGE_SIZE>) -> std::result::Result<Self, Error> {
    let mut reader = page.scanner();
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
