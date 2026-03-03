use std::{
  collections::HashSet,
  sync::{Arc, Mutex},
};

use crate::{
  buffer_pool::{BufferPool, PageSlotWrite},
  disk::{PageScanner, PageWriter, PAGE_SIZE},
  error::Result,
  serialize::{Serializable, SerializeFrom, SerializeType},
  utils::ShortenedMutex,
  wal::WAL,
};

pub struct FreeBlock {
  next: Option<usize>,
  prev: Option<usize>,
  list: Vec<usize>,
}
impl FreeBlock {
  fn new(prev: Option<usize>) -> Self {
    Self {
      next: None,
      prev,
      list: Default::default(),
    }
  }
  fn is_available(&self) -> bool {
    24 + (self.list.len() + 1) * 8 >= PAGE_SIZE
  }
}
impl Serializable for FreeBlock {
  fn get_type() -> SerializeType {
    SerializeType::Free
  }

  fn write_at(&self, writer: &mut PageWriter) -> Result {
    writer.write_usize(self.next.unwrap_or(0))?;
    writer.write_usize(self.prev.unwrap_or(0))?;
    writer.write_usize(self.list.len())?;
    for i in self.list.iter() {
      writer.write_usize(*i)?;
    }
    Ok(())
  }

  fn read_from(reader: &mut PageScanner) -> Result<Self> {
    let next = reader.read_usize()?;
    let prev = reader.read_usize()?;
    let len = reader.read_usize()?;
    let mut list = Vec::new();
    for _ in 0..len {
      list.push(reader.read_usize()?);
    }
    Ok(Self {
      list,
      next: (next != 0).then(|| next),
      prev: (prev != 0).then(|| prev),
    })
  }
}

pub const FREE_LIST_HEAD: usize = 1;

struct FreeState {
  block: FreeBlock,
  index: usize,
  file_end: usize,
}
pub struct FreeList {
  state: Mutex<FreeState>,
  buffer_pool: Arc<BufferPool>,
  wal: Arc<WAL>,
}
impl FreeList {
  pub fn replay(buffer_pool: Arc<BufferPool>, wal: Arc<WAL>) -> Result<Self> {
    let file_end = buffer_pool.disk_len()?;
    if file_end == 0 {
      let state = FreeState {
        block: FreeBlock::new(None),
        index: FREE_LIST_HEAD,
        file_end: FREE_LIST_HEAD + 1,
      };
      {
        let mut slot = buffer_pool.read(FREE_LIST_HEAD)?.for_write();
        slot.as_mut().serialize_from(&state.block)?;
        wal.append_insert(0, FREE_LIST_HEAD, slot.as_ref())?;
      }
      return Ok(Self {
        state: Mutex::new(state),
        buffer_pool,
        wal,
      });
    }

    let mut index = FREE_LIST_HEAD;

    let (block, index) = loop {
      let block: FreeBlock =
        buffer_pool.read(index)?.for_read().as_ref().deserialize()?;
      match block.next {
        Some(i) => index = i,
        None => break (block, index),
      }
    };

    let state = FreeState {
      block,
      index,
      file_end,
    };

    Ok(Self {
      state: Mutex::new(state),
      buffer_pool,
      wal,
    })
  }

  pub fn get_all(&self) -> Result<(HashSet<usize>, Vec<usize>)> {
    let mut index = Some(FREE_LIST_HEAD);
    let mut visited = vec![];
    let mut set = HashSet::new();
    while let Some(i) = index {
      visited.push(i);
      let block = self
        .buffer_pool
        .read(i)?
        .for_read()
        .as_ref()
        .deserialize::<FreeBlock>()?;
      set.extend(block.list);
      index = block.next
    }
    Ok((set, visited))
  }

  pub fn alloc(&self) -> Result<PageSlotWrite<'_>> {
    let mut state = self.state.l();
    if let Some(index) = state.block.list.pop() {
      let mut free_slot = self.buffer_pool.read(state.index)?.for_write();
      free_slot.as_mut().serialize_from(&state.block)?;
      self.wal.append_insert(0, state.index, free_slot.as_ref())?;
      return Ok(self.buffer_pool.read(index)?.for_write());
    }

    if let Some(prev) = state.block.prev {
      let mut prev_slot = self.buffer_pool.read(prev)?.for_write();
      let mut prev_block: FreeBlock = prev_slot.as_ref().deserialize()?;
      prev_block.next = None;

      let empty_index = state.index;
      state.block = prev_block;
      state.index = prev;

      prev_slot.as_mut().serialize_from(&state.block)?;
      self.wal.append_insert(0, state.index, prev_slot.as_ref())?;

      return Ok(self.buffer_pool.read(empty_index)?.for_write());
    }

    let index = state.file_end;
    state.file_end += 1;
    Ok(self.buffer_pool.read(index)?.for_write())
  }

  pub fn release(&self, index: usize) -> Result {
    let mut state = self.state.l();
    if !state.block.is_available() {
      state.block.list.push(index);
      let mut free_slot = self.buffer_pool.read(state.index)?.for_write();
      free_slot.as_mut().serialize_from(&state.block)?;
      self.wal.append_insert(0, state.index, free_slot.as_ref())?;
      return Ok(());
    }

    let mut new_slot = self.buffer_pool.read(index)?.for_write();
    let new_free = FreeBlock::new(Some(state.index));
    new_slot.as_mut().serialize_from(&new_free)?;
    self.wal.append_insert(0, index, new_slot.as_ref())?;

    state.block.next = Some(index);
    let mut free_slot = self.buffer_pool.read(state.index)?.for_write();
    free_slot.as_mut().serialize_from(&state.block)?;
    self.wal.append_insert(0, state.index, free_slot.as_ref())?;

    state.block = new_free;
    state.index = index;
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use crate::{disk::Page, serialize::SerializeFrom};

  use super::*;

  #[test]
  fn test_free_page_roundtrip() {
    let mut page = Page::new();
    let i = 99;
    let free = FreeBlock::new(Some(i));
    page.serialize_from(&free).expect("serialize error");
    let decoded: FreeBlock = page.deserialize().expect("deserialize error");
    assert_eq!(decoded.prev, Some(i));
    assert_eq!(decoded.next, None);
  }

  #[test]
  fn test_free_page_full() {
    let mut page = Page::new();
    let mut free = FreeBlock::new(None);
    let mut i = 0;
    while !free.is_available() {
      free.list.push(i);
      i += 1;
    }

    page.serialize_from(&free).expect("serialize error");
    let decoded: FreeBlock = page.deserialize().expect("deserialize error");
    assert_eq!(decoded.is_available(), true);

    for c in 0..i {
      assert_eq!(decoded.list[c], c);
    }
    assert_eq!(decoded.list.len(), i);
    assert_eq!(decoded.prev, None);
    assert_eq!(decoded.next, None);
  }
}
