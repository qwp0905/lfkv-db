use std::sync::{
  atomic::{AtomicUsize, Ordering},
  RwLock, RwLockReadGuard, RwLockWriteGuard,
};

use crate::{
  disk::{Page, PageRef, PAGE_SIZE},
  utils::{Bitmap, ShortenedRwLock},
};
pub struct PageSlot<'a> {
  page: &'a RwLock<PageRef<PAGE_SIZE>>,
  frame_id: usize,
  dirty: &'a Bitmap,
  index: usize,
  pin: &'a AtomicUsize,
}
impl<'a> PageSlot<'a> {
  pub fn new(
    page: &'a RwLock<PageRef<PAGE_SIZE>>,
    frame_id: usize,
    dirty: &'a Bitmap,
    index: usize,
    pin: &'a AtomicUsize,
  ) -> Self {
    Self {
      page,
      frame_id,
      dirty,
      index,
      pin,
    }
  }
  pub fn get_index(&self) -> usize {
    self.index
  }

  pub fn for_read<'b>(self) -> PageSlotRead<'b>
  where
    'a: 'b,
  {
    PageSlotRead {
      guard: self.page.rl(),
      pin: self.pin,
    }
  }
  pub fn for_write<'b>(self) -> PageSlotWrite<'b>
  where
    'a: 'b,
  {
    PageSlotWrite {
      guard: self.page.wl(),
      dirty: self.dirty,
      frame_id: self.frame_id,
      index: self.index,
      pin: self.pin,
    }
  }
}
pub struct PageSlotWrite<'a> {
  guard: RwLockWriteGuard<'a, PageRef<PAGE_SIZE>>,
  index: usize,
  dirty: &'a Bitmap,
  frame_id: usize,
  pin: &'a AtomicUsize,
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
    self.pin.fetch_sub(1, Ordering::Release);
  }
}
pub struct PageSlotRead<'a> {
  guard: RwLockReadGuard<'a, PageRef<PAGE_SIZE>>,
  pin: &'a AtomicUsize,
}
impl<'a> AsRef<Page<PAGE_SIZE>> for PageSlotRead<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    self.guard.as_ref()
  }
}
impl<'a> Drop for PageSlotRead<'a> {
  fn drop(&mut self) {
    self.pin.fetch_sub(1, Ordering::Release);
  }
}
