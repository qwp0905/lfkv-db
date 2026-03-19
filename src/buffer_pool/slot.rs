use std::{
  mem::{transmute, ManuallyDrop},
  sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crossbeam::utils::Backoff;

use super::{Frame, FrameState, Shard, TempFrameState};
use crate::{
  disk::{DiskController, Page, PageRef, PAGE_SIZE},
  utils::{Bitmap, ShortenedMutex, ShortenedRwLock, UnsafeBorrow},
};

pub enum Slot {
  Temp(TempSlot),
  Page(PageSlot),
}
unsafe impl Send for Slot {}
impl Slot {
  pub fn temp(
    state: Arc<TempFrameState>,
    index: usize,
    disk: &DiskController<PAGE_SIZE>,
    shard: &Mutex<Shard>,
    is_peek: bool,
  ) -> Self {
    Self::Temp(TempSlot {
      state,
      index,
      disk,
      shard,
      is_peek,
    })
  }
  pub fn page(frame: &RwLock<Frame>, dirty: &Bitmap, state: Arc<FrameState>) -> Self {
    Self::Page(PageSlot {
      frame,
      dirty,
      state,
    })
  }
  pub fn for_read<'a>(self) -> ReadableSlot<'a> {
    match self {
      Slot::Temp(temp) => ReadableSlot::Temp(temp.for_read()),
      Slot::Page(page) => ReadableSlot::Page(page.for_read()),
    }
  }

  pub fn for_write<'a>(self) -> WritableSlot<'a> {
    match self {
      Slot::Temp(temp) => WritableSlot::Temp(temp.for_write()),
      Slot::Page(page) => WritableSlot::Page(page.for_write()),
    }
  }

  pub fn release(self) {
    match self {
      Slot::Page(page) => page.state.unpin(),
      Slot::Temp(temp) => {
        if !temp.is_peek {
          return temp.state.unpin();
        }

        let backoff = Backoff::new();
        while !temp.state.try_evict() {
          backoff.snooze();
        }
        if temp.state.is_dirty() {
          let _ = temp
            .disk
            .borrow_unsafe()
            .write(temp.index, &temp.state.for_read());
        }
        temp.shard.borrow_unsafe().l().remove_temp(temp.index);
      }
    }
  }
}
pub enum WritableSlot<'a> {
  Temp(TempSlotWrite<'a>),
  Page(PageSlotWrite<'a>),
}
impl<'a> AsMut<Page<PAGE_SIZE>> for WritableSlot<'a> {
  fn as_mut(&mut self) -> &mut Page<PAGE_SIZE> {
    match self {
      WritableSlot::Temp(temp) => temp.guard.as_mut(),
      WritableSlot::Page(page) => page.guard.page_ref_mut().as_mut(),
    }
  }
}
impl<'a> AsRef<Page<PAGE_SIZE>> for WritableSlot<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    match self {
      WritableSlot::Temp(temp) => temp.guard.as_ref(),
      WritableSlot::Page(page) => page.guard.page_ref().as_ref(),
    }
  }
}
impl<'a> WritableSlot<'a> {
  pub fn get_index(&self) -> usize {
    match self {
      WritableSlot::Temp(temp) => temp.index,
      WritableSlot::Page(page) => page.guard.get_index(),
    }
  }
}

pub enum ReadableSlot<'a> {
  Temp(TempSlotRead<'a>),
  Page(PageSlotRead<'a>),
}
impl<'a> AsRef<Page<PAGE_SIZE>> for ReadableSlot<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    match self {
      ReadableSlot::Temp(temp) => temp.guard.as_ref(),
      ReadableSlot::Page(page) => page.guard.page_ref().as_ref(),
    }
  }
}

pub struct PageSlot {
  frame: *const RwLock<Frame>,
  dirty: *const Bitmap,
  state: Arc<FrameState>,
}
impl PageSlot {
  fn for_read<'a>(self) -> PageSlotRead<'a> {
    PageSlotRead {
      guard: self.frame.borrow_unsafe().rl(),
      state: self.state,
    }
  }

  fn for_write<'a>(self) -> PageSlotWrite<'a> {
    PageSlotWrite {
      guard: self.frame.borrow_unsafe().wl(),
      dirty: self.dirty,
      state: self.state,
    }
  }
}
pub struct PageSlotWrite<'a> {
  guard: RwLockWriteGuard<'a, Frame>,
  dirty: *const Bitmap,
  state: Arc<FrameState>,
}

impl<'a> Drop for PageSlotWrite<'a> {
  fn drop(&mut self) {
    self.dirty.borrow_unsafe().insert(self.state.get_frame_id());
    self.state.unpin();
  }
}

pub struct PageSlotRead<'a> {
  guard: RwLockReadGuard<'a, Frame>,
  state: Arc<FrameState>,
}
impl<'a> Drop for PageSlotRead<'a> {
  fn drop(&mut self) {
    self.state.unpin();
  }
}

pub struct TempSlot {
  state: Arc<TempFrameState>,
  index: usize,
  disk: *const DiskController<PAGE_SIZE>,
  shard: *const Mutex<Shard>,
  is_peek: bool,
}
impl TempSlot {
  fn for_read<'a>(self) -> TempSlotRead<'a> {
    TempSlotRead {
      guard: ManuallyDrop::new(unsafe { transmute(self.state.for_read()) }),
      state: self.state.clone(),
      index: self.index,
      disk: self.disk,
      shard: self.shard,
      is_peek: self.is_peek,
    }
  }

  fn for_write<'a>(self) -> TempSlotWrite<'a> {
    TempSlotWrite {
      guard: ManuallyDrop::new(unsafe { transmute(self.state.for_write()) }),
      state: self.state.clone(),
      index: self.index,
      disk: self.disk,
      shard: self.shard,
      is_peek: self.is_peek,
    }
  }
}

pub struct TempSlotWrite<'a> {
  state: Arc<TempFrameState>,
  guard: ManuallyDrop<RwLockWriteGuard<'a, PageRef<PAGE_SIZE>>>,
  index: usize,
  disk: *const DiskController<PAGE_SIZE>,
  shard: *const Mutex<Shard>,
  is_peek: bool,
}

impl<'a> TempSlotWrite<'a> {
  fn release(&mut self) {
    unsafe { ManuallyDrop::drop(&mut self.guard) };
    let backoff = Backoff::new();
    while !self.state.try_evict() {
      backoff.snooze();
    }
    let _ = self
      .disk
      .borrow_unsafe()
      .write(self.index, &self.state.for_read());
    self.shard.borrow_unsafe().l().remove_temp(self.index);
  }
}
impl<'a> Drop for TempSlotWrite<'a> {
  fn drop(&mut self) {
    if self.is_peek {
      return self.release();
    }
    self.state.mark_dirty();
    self.state.unpin();
    unsafe { ManuallyDrop::drop(&mut self.guard) };
  }
}

pub struct TempSlotRead<'a> {
  state: Arc<TempFrameState>,
  guard: ManuallyDrop<RwLockReadGuard<'a, PageRef<PAGE_SIZE>>>,
  index: usize,
  disk: *const DiskController<PAGE_SIZE>,
  shard: *const Mutex<Shard>,
  is_peek: bool,
}
impl<'a> TempSlotRead<'a> {
  fn release(&mut self) {
    unsafe { ManuallyDrop::drop(&mut self.guard) };
    let backoff = Backoff::new();
    while !self.state.try_evict() {
      backoff.snooze();
    }
    if self.state.is_dirty() {
      let _ = self
        .disk
        .borrow_unsafe()
        .write(self.index, &self.state.for_read());
    }
    self.shard.borrow_unsafe().l().remove_temp(self.index);
  }
}
impl<'a> Drop for TempSlotRead<'a> {
  fn drop(&mut self) {
    if self.is_peek {
      return self.release();
    }

    self.state.unpin();
    unsafe { ManuallyDrop::drop(&mut self.guard) };
  }
}
