use std::{
  mem::replace,
  sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use super::FrameState;
use crate::{
  disk::{Page, PageRef, PAGE_SIZE},
  utils::{Bitmap, ShortenedRwLock},
};

pub struct Frame {
  page: PageRef<PAGE_SIZE>,
  /**
   * index can be wrong if nothing allocated.
   * only lru table is the single truth source.
   */
  index: usize,
}
impl Frame {
  pub fn empty(page: PageRef<PAGE_SIZE>) -> Self {
    Self { page, index: 0 }
  }
  pub fn replace(
    &mut self,
    index: usize,
    page: PageRef<PAGE_SIZE>,
  ) -> PageRef<PAGE_SIZE> {
    self.index = index;
    replace(&mut self.page, page)
  }
  pub fn get_index(&self) -> usize {
    self.index
  }
  pub fn page_ref(&self) -> &PageRef<PAGE_SIZE> {
    &self.page
  }
}

pub struct PageSlot<'a> {
  frame: &'a RwLock<Frame>,
  dirty: &'a Bitmap,
  state: Arc<FrameState>,
}
impl<'a> PageSlot<'a> {
  pub fn new(
    frame: &'a RwLock<Frame>,
    dirty: &'a Bitmap,
    state: Arc<FrameState>,
  ) -> Self {
    Self {
      frame,
      dirty,
      state,
    }
  }

  pub fn for_read<'b>(self) -> PageSlotRead<'b>
  where
    'a: 'b,
  {
    PageSlotRead {
      guard: self.frame.rl(),
      state: self.state,
    }
  }
  pub fn for_write<'b>(self) -> PageSlotWrite<'b>
  where
    'a: 'b,
  {
    PageSlotWrite {
      guard: self.frame.wl(),
      dirty: self.dirty,
      state: self.state,
    }
  }
}
pub struct PageSlotWrite<'a> {
  guard: RwLockWriteGuard<'a, Frame>,
  dirty: &'a Bitmap,
  state: Arc<FrameState>,
}
impl<'a> PageSlotWrite<'a> {
  pub fn get_index(&self) -> usize {
    self.guard.index
  }
}
impl<'a> AsMut<Page<PAGE_SIZE>> for PageSlotWrite<'a> {
  fn as_mut(&mut self) -> &mut Page<PAGE_SIZE> {
    self.guard.page.as_mut()
  }
}
impl<'a> AsRef<Page<PAGE_SIZE>> for PageSlotWrite<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    self.guard.page.as_ref()
  }
}
impl<'a> Drop for PageSlotWrite<'a> {
  fn drop(&mut self) {
    self.dirty.insert(self.state.get_frame_id());
    self.state.unpin();
  }
}
pub struct PageSlotRead<'a> {
  guard: RwLockReadGuard<'a, Frame>,
  state: Arc<FrameState>,
}
impl<'a> AsRef<Page<PAGE_SIZE>> for PageSlotRead<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    self.guard.page.as_ref()
  }
}
impl<'a> Drop for PageSlotRead<'a> {
  fn drop(&mut self) {
    self.state.unpin();
  }
}
