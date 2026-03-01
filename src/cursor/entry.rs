use std::{collections::VecDeque, mem::replace};

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
  pub fn get_data(&self) -> Option<&Vec<u8>> {
    match self {
      RecordData::Data(data) => Some(data),
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
}

#[derive(Debug)]
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

  pub fn get_versions(&self) -> impl Iterator<Item = &VersionRecord> {
    self.versions.iter()
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
mod tests {
  use crate::{disk::Page, serialize::SerializeFrom};

  use super::*;

  #[test]
  fn test_empty_entry_roundtrip() {
    let mut page = Page::new();
    let entry = DataEntry::new();
    page.serialize_from(&entry).expect("serialize error");

    let decoded: DataEntry = page.deserialize().expect("deserialize error");
    assert!(decoded.is_empty());
    assert_eq!(decoded.get_next(), None);
  }

  #[test]
  fn test_entry_with_data_roundtrip() {
    let mut page = Page::new();
    let mut entry = DataEntry::new();
    entry.append(VersionRecord::new(
      1,
      100,
      RecordData::Data(vec![10, 20, 30]),
    ));
    page.serialize_from(&entry).expect("serialize error");

    let decoded: DataEntry = page.deserialize().expect("deserialize error");
    assert!(!decoded.is_empty());
    assert_eq!(decoded.get_last_owner(), Some(1));

    let records: Vec<_> = decoded.get_versions().collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].owner, 1);
    assert_eq!(records[0].version, 100);
    match &records[0].data {
      RecordData::Data(d) => assert_eq!(d, &vec![10, 20, 30]),
      RecordData::Tombstone => panic!("expected Data"),
    }
  }

  #[test]
  fn test_entry_with_tombstone_roundtrip() {
    let mut page = Page::new();
    let mut entry = DataEntry::new();
    entry.append(VersionRecord::new(2, 200, RecordData::Tombstone));
    page.serialize_from(&entry).expect("serialize error");

    let decoded: DataEntry = page.deserialize().expect("deserialize error");
    assert!(!decoded.is_empty());
    assert_eq!(decoded.get_last_owner(), Some(2));

    let records: Vec<_> = decoded.get_versions().collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].owner, 2);
    match &records[0].data {
      RecordData::Data(_) => panic!("expected Tombstone"),
      RecordData::Tombstone => {}
    }
  }

  #[test]
  fn test_entry_with_next_roundtrip() {
    let mut page = Page::new();
    let mut entry = DataEntry::new();
    entry.set_next(42);
    entry.append(VersionRecord::new(1, 10, RecordData::Data(vec![1])));
    page.serialize_from(&entry).expect("serialize error");

    let decoded: DataEntry = page.deserialize().expect("deserialize error");
    assert_eq!(decoded.get_next(), Some(42));
  }

  #[test]
  fn test_entry_multiple_versions_roundtrip() {
    let mut page = Page::new();
    let mut entry = DataEntry::new();
    entry.append(VersionRecord::new(3, 300, RecordData::Data(vec![3])));
    entry.append(VersionRecord::new(2, 200, RecordData::Tombstone));
    entry.append(VersionRecord::new(1, 100, RecordData::Data(vec![1, 2])));
    page.serialize_from(&entry).expect("serialize error");

    let mut decoded: DataEntry = page.deserialize().expect("deserialize error");
    assert!(!decoded.is_empty());
    assert_eq!(decoded.get_last_owner(), Some(1));
    assert_eq!(decoded.get_next(), None);

    // verify all 3 versions survived by filtering none
    let removed = decoded.filter_aborted(|_| false);
    assert!(!removed);
    assert!(!decoded.is_empty());
  }
}
