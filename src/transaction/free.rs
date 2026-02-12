use crate::{
  disk::{PageScanner, PageWriter},
  serialize::{Serializable, SerializeType},
  Result,
};

pub struct FreePage {
  next: usize,
}
impl FreePage {
  pub fn new(next: usize) -> Self {
    Self { next }
  }
  pub fn get_next(&self) -> usize {
    self.next
  }
}
impl Serializable for FreePage {
  fn get_type() -> SerializeType {
    SerializeType::Free
  }

  fn write_at(&self, writer: &mut PageWriter) -> Result {
    writer.write_usize(self.next)
  }

  fn read_from(reader: &mut PageScanner) -> Result<Self> {
    Ok(Self {
      next: reader.read_usize()?,
    })
  }
}
