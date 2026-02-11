use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{
  disk::{PageRef, PAGE_SIZE},
  utils::{Bitmap, ShortenedRwLock},
  Page,
};
pub struct PageSlot<'a> {
  page: &'a RwLock<PageRef<PAGE_SIZE>>,
  frame_id: usize,
  dirty: &'a Bitmap,
  index: usize,
}
impl<'a> PageSlot<'a> {
  pub fn new(
    page: &'a RwLock<PageRef<PAGE_SIZE>>,
    frame_id: usize,
    dirty: &'a Bitmap,
    index: usize,
  ) -> Self {
    Self {
      page,
      frame_id,
      dirty,
      index,
    }
  }
  pub fn get_index(&self) -> usize {
    self.index
  }

  pub fn for_read(&self) -> PageSlotRead<'a> {
    PageSlotRead {
      guard: self.page.rl(),
    }
  }
  pub fn for_write(&self) -> PageSlotWrite<'a> {
    PageSlotWrite {
      guard: self.page.wl(),
      dirty: self.dirty,
      frame_id: self.frame_id,
      index: self.index,
    }
  }
}
pub struct PageSlotWrite<'a> {
  guard: RwLockWriteGuard<'a, PageRef<PAGE_SIZE>>,
  index: usize,
  dirty: &'a Bitmap,
  frame_id: usize,
}
impl<'a> PageSlotWrite<'a> {
  pub fn get_index(&self) -> usize {
    self.index
  }
}
impl<'a> AsMut<Page<PAGE_SIZE>> for PageSlotWrite<'a> {
  fn as_mut(&mut self) -> &mut Page<PAGE_SIZE> {
    self.guard.as_mut()
  }
}
impl<'a> AsRef<Page<PAGE_SIZE>> for PageSlotWrite<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    self.guard.as_ref()
  }
}
impl<'a> Drop for PageSlotWrite<'a> {
  fn drop(&mut self) {
    self.dirty.insert(self.frame_id);
  }
}
pub struct PageSlotRead<'a> {
  guard: RwLockReadGuard<'a, PageRef<PAGE_SIZE>>,
}
impl<'a> AsRef<Page<PAGE_SIZE>> for PageSlotRead<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    self.guard.as_ref()
  }
}
