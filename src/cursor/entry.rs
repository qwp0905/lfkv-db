use std::collections::VecDeque;

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
    versions.push_back(version);
    Self {
      next: None,
      versions,
    }
  }

  pub fn drain(&mut self) -> impl Iterator<Item = VersionRecord> + '_ {
    self.versions.drain(..)
  }
  pub fn set_records(&mut self, records: VecDeque<VersionRecord>) {
    self.versions = records;
  }

  pub fn is_available(&self, record: &VersionRecord) -> bool {
    let byte_len = 8 + 8 + self.versions.iter().fold(0, |a, c| a + c.byte_len());
    record.byte_len() + byte_len < SERIALIZABLE_BYTES
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

  pub fn len(&self) -> usize {
    self.versions.len()
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

// #[cfg(test)]
// mod tests {
//   use crate::{disk::Page, serialize::SerializeFrom};

//   use super::*;

//   #[test]
//   fn test_empty_entry_roundtrip() {
//     let mut page = Page::new();
//     let entry = DataEntry::new();
//     page.serialize_from(&entry).expect("serialize error");

//     let decoded: DataEntry = page.deserialize().expect("deserialize error");
//     assert!(decoded.is_empty());
//     assert_eq!(decoded.get_next(), None);
//   }

//   #[test]
//   fn test_entry_with_data_roundtrip() {
//     let mut page = Page::new();
//     let mut entry = DataEntry::new();
//     entry.append(VersionRecord::new(
//       1,
//       100,
//       RecordData::Data(vec![10, 20, 30]),
//     ));
//     page.serialize_from(&entry).expect("serialize error");

//     let decoded: DataEntry = page.deserialize().expect("deserialize error");
//     assert!(!decoded.is_empty());
//     assert_eq!(decoded.get_last_owner(), Some(1));

//     let records: Vec<_> = decoded.get_versions().collect();
//     assert_eq!(records.len(), 1);
//     assert_eq!(records[0].owner, 1);
//     assert_eq!(records[0].version, 100);
//     match &records[0].data {
//       RecordData::Data(d) => assert_eq!(d, &vec![10, 20, 30]),
//       RecordData::Tombstone => panic!("expected Data"),
//     }
//   }

//   #[test]
//   fn test_entry_with_tombstone_roundtrip() {
//     let mut page = Page::new();
//     let mut entry = DataEntry::new();
//     entry.append(VersionRecord::new(2, 200, RecordData::Tombstone));
//     page.serialize_from(&entry).expect("serialize error");

//     let decoded: DataEntry = page.deserialize().expect("deserialize error");
//     assert!(!decoded.is_empty());
//     assert_eq!(decoded.get_last_owner(), Some(2));

//     let records: Vec<_> = decoded.get_versions().collect();
//     assert_eq!(records.len(), 1);
//     assert_eq!(records[0].owner, 2);
//     match &records[0].data {
//       RecordData::Data(_) => panic!("expected Tombstone"),
//       RecordData::Tombstone => {}
//     }
//   }

//   #[test]
//   fn test_entry_with_next_roundtrip() {
//     let mut page = Page::new();
//     let mut entry = DataEntry::new();
//     entry.set_next(42);
//     entry.append(VersionRecord::new(1, 10, RecordData::Data(vec![1])));
//     page.serialize_from(&entry).expect("serialize error");

//     let decoded: DataEntry = page.deserialize().expect("deserialize error");
//     assert_eq!(decoded.get_next(), Some(42));
//   }

//   #[test]
//   fn test_entry_multiple_versions_roundtrip() {
//     let mut page = Page::new();
//     let mut entry = DataEntry::new();
//     entry.append(VersionRecord::new(3, 300, RecordData::Data(vec![3])));
//     entry.append(VersionRecord::new(2, 200, RecordData::Tombstone));
//     entry.append(VersionRecord::new(1, 100, RecordData::Data(vec![1, 2])));
//     page.serialize_from(&entry).expect("serialize error");

//     let mut decoded: DataEntry = page.deserialize().expect("deserialize error");
//     assert!(!decoded.is_empty());
//     assert_eq!(decoded.get_last_owner(), Some(1));
//     assert_eq!(decoded.get_next(), None);

//     // verify all 3 versions survived by filtering none
//     let removed = decoded.filter_aborted(|_| false);
//     assert!(!removed);
//     assert!(!decoded.is_empty());
//   }
// }
