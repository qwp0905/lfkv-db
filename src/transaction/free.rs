use std::{
  mem::replace,
  sync::{Arc, RwLock},
};

use crate::{
  buffer_pool::{BufferPool, PageSlot},
  disk::{PageScanner, PageWriter},
  error::{Error, Result},
  serialize::{Serializable, SerializeFrom, SerializeType},
  utils::ShortenedRwLock,
  wal::WAL,
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

pub struct FreeList {
  last_free: Arc<RwLock<usize>>,
  buffer_pool: Arc<BufferPool>,
  wal: Arc<WAL>,
}
impl FreeList {
  pub fn new(
    last_free: Arc<RwLock<usize>>,
    buffer_pool: Arc<BufferPool>,
    wal: Arc<WAL>,
  ) -> Self {
    Self {
      last_free,
      buffer_pool,
      wal,
    }
  }

  pub fn alloc(&self) -> Result<PageSlot<'_>> {
    let mut index = self.last_free.wl();
    let slot = self.buffer_pool.read(*index)?;
    *index = slot
      .for_read()
      .as_ref()
      .deserialize::<FreePage>()?
      .get_next();
    Ok(slot)
  }

  pub fn release(&self, index: usize) -> Result {
    let page = self.buffer_pool.read(index)?;
    let mut prev = self.last_free.wl();
    match self.wal.append_free(index) {
      Ok(_) => {}
      Err(Error::WALCapacityExceeded) => self.wal.append_free(index)?,
      Err(err) => return Err(err),
    };
    let free = FreePage::new(replace(&mut prev, index));
    page.for_write().as_mut().serialize_from(&free)?;
    Ok(())
  }
}
