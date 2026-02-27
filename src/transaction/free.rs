use std::{
  mem::replace,
  sync::{Arc, RwLock},
};

use crate::{
  buffer_pool::{BufferPool, PageSlot},
  disk::{PageScanner, PageWriter},
  error::Result,
  serialize::{Serializable, SerializeFrom, SerializeType},
  utils::ShortenedRwLock,
  wal::WAL,
};

struct FreeState {
  last_free: usize,
  next_index: usize,
}
impl FreeState {
  fn new(last_free: usize, next_index: usize) -> Self {
    Self {
      last_free,
      next_index,
    }
  }
}

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

#[cfg(test)]
mod tests {
  use crate::{disk::Page, serialize::SerializeFrom};

  use super::*;

  #[test]
  fn test_free_page_roundtrip() {
    let mut page = Page::new();
    let free = FreePage::new(99);
    page.serialize_from(&free).expect("serialize error");

    let decoded: FreePage = page.deserialize().expect("deserialize error");
    assert_eq!(decoded.get_next(), 99);
  }

  #[test]
  fn test_free_page_zero_next() {
    let mut page = Page::new();
    let free = FreePage::new(0);
    page.serialize_from(&free).expect("serialize error");

    let decoded: FreePage = page.deserialize().expect("deserialize error");
    assert_eq!(decoded.get_next(), 0);
  }
}

pub struct FreeList {
  state: RwLock<FreeState>,
  buffer_pool: Arc<BufferPool>,
  wal: Arc<WAL>,
}
impl FreeList {
  pub fn new(
    last_free: usize,
    next_index: usize,
    buffer_pool: Arc<BufferPool>,
    wal: Arc<WAL>,
  ) -> Self {
    Self {
      state: RwLock::new(FreeState::new(last_free, next_index)),
      buffer_pool,
      wal,
    }
  }

  pub fn get_last_free(&self) -> usize {
    self.state.rl().last_free
  }

  pub fn set_next_index(&self, index: usize) {
    self.state.wl().next_index = index;
  }

  pub fn alloc(&self) -> Result<PageSlot<'_>> {
    let mut state = self.state.wl();
    if state.last_free == 0 {
      let index = state.next_index;
      state.next_index += 1;
      return self.buffer_pool.read(index);
    }

    let slot = self.buffer_pool.read(state.last_free)?;
    state.last_free = slot
      .for_read()
      .as_ref()
      .deserialize::<FreePage>()?
      .get_next();
    self.wal.append_free(state.last_free)?;
    return Ok(slot);
  }

  pub fn release(&self, index: usize) -> Result {
    let page = self.buffer_pool.read(index)?;
    let mut state = self.state.wl();
    self.wal.append_free(index)?;
    let free = FreePage::new(replace(&mut state.last_free, index));
    page.for_write().as_mut().serialize_from(&free)?;
    Ok(())
  }
}
