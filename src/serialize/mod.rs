use crate::{
  disk::{Page, PageScanner, PageWriter, PAGE_SIZE},
  error::{Error, Result},
};

pub enum SerializeType {
  Header,
  CursorNode,
  DataEntry,
  Free,
}
impl From<SerializeType> for u8 {
  fn from(value: SerializeType) -> Self {
    match value {
      SerializeType::Header => 1,
      SerializeType::CursorNode => 2,
      SerializeType::DataEntry => 3,
      SerializeType::Free => 4,
    }
  }
}

pub const SERIALIZABLE_BYTES: usize = PAGE_SIZE - 1;

pub trait Serializable: Sized {
  fn get_type() -> SerializeType;
  fn serialize_at(&self, page: &mut Page<PAGE_SIZE>) -> Result {
    let mut writer = page.writer();
    writer.write(&[u8::from(Self::get_type())])?;
    self.write_at(&mut writer)?;
    Ok(())
  }
  fn write_at(&self, writer: &mut PageWriter) -> Result;
  fn read_from(reader: &mut PageScanner) -> Result<Self>;
  fn deserialize(value: &Page<PAGE_SIZE>) -> Result<Self> {
    let mut reader = value.scanner();
    if u8::from(Self::get_type()) != reader.read()? {
      return Err(Error::InvalidFormat("block type not matched"));
    }

    Self::read_from(&mut reader)
  }
}
impl Page<PAGE_SIZE> {
  pub fn deserialize<T>(&self) -> Result<T>
  where
    T: Serializable,
  {
    T::deserialize(self)
  }
}

pub trait SerializeFrom<T: Serializable> {
  fn serialize_from(&mut self, target: &T) -> Result;
}
impl<T: Serializable> SerializeFrom<T> for Page<PAGE_SIZE> {
  fn serialize_from(&mut self, target: &T) -> Result {
    target.serialize_at(self)
  }
}
