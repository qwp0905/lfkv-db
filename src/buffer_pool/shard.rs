use std::{
  hash::BuildHasher,
  mem::replace,
  sync::{Arc, MutexGuard},
};

use crate::{
  buffer_pool::lru::LRUTable,
  disk::{DiskController, PageRef, PAGE_SIZE},
  Bitmap, Result,
};

pub struct CachedPage<'a> {
  guard: MutexGuard<'a, BufferPoolShard>,
  value: *mut PageRef<PAGE_SIZE>,
  frame_id: usize,
  dirty: bool,
}
impl<'a> CachedPage<'a> {
  pub fn new(
    guard: MutexGuard<'a, BufferPoolShard>,
    value: *mut PageRef<PAGE_SIZE>,
    frame_id: usize,
    dirty: bool,
  ) -> Self {
    Self {
      guard,
      value,
      frame_id,
      dirty,
    }
  }
}
impl<'a> Drop for CachedPage<'a> {
  #[inline]
  fn drop(&mut self) {
    if self.dirty {
      self.guard.dirty.insert(self.frame_id);
    }
  }
}
impl<'a> AsRef<PageRef<PAGE_SIZE>> for CachedPage<'a> {
  #[inline]
  fn as_ref(&self) -> &PageRef<PAGE_SIZE> {
    unsafe { &*self.value }
  }
}

impl<'a> AsMut<PageRef<PAGE_SIZE>> for CachedPage<'a> {
  #[inline]
  fn as_mut(&mut self) -> &mut PageRef<PAGE_SIZE> {
    self.dirty = true;
    unsafe { &mut *self.value }
  }
}

pub struct BufferPoolShard {
  frame: Vec<PageRef<PAGE_SIZE>>,
  table: LRUTable<usize, usize>,
  dirty: Bitmap,

  disk: Arc<DiskController<PAGE_SIZE>>,
}
impl BufferPoolShard {
  pub fn new(disk: Arc<DiskController<PAGE_SIZE>>, cap: usize) -> Self {
    Self {
      frame: Vec::with_capacity(cap),
      table: LRUTable::new(cap),
      dirty: Bitmap::new(cap),
      disk,
    }
  }

  #[inline]
  fn get_frame<S>(&mut self, index: usize, hash: u64, hasher: &S) -> Result<usize>
  where
    S: BuildHasher,
  {
    if let Some(id) = self.table.get(&index, hash, hasher) {
      return Ok(*id);
    };

    let page = self.disk.read(index)?;
    if !self.table.is_full() {
      let id = self.frame.len();
      self.frame.push(page);
      self.table.insert(index, id, hash, hasher);
      return Ok(id);
    };

    let (_, evicted) = self.table.evict(hasher).unwrap();
    self.table.insert(index, evicted, hash, hasher);
    let evcited_page = replace(&mut self.frame[evicted], page);
    if self.dirty.remove(evicted) {
      self.disk.write(&evcited_page)?;
    }
    Ok(evicted)
  }

  pub fn get_mut<S>(
    &mut self,
    index: usize,
    hash: u64,
    hasher: &S,
  ) -> Result<(usize, bool, &mut PageRef<PAGE_SIZE>)>
  where
    S: BuildHasher,
  {
    let id = self.get_frame(index, hash, hasher)?;
    Ok((id, self.dirty.contains(id), &mut self.frame[id]))
  }

  pub fn flush(&mut self) -> Result {
    for i in self.dirty.iter() {
      self.disk.write(&self.frame[i])?;
    }
    self.disk.fsync()?;
    self.dirty.clear();
    Ok(())
  }
}
