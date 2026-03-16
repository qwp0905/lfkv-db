use std::sync::{
  atomic::{AtomicBool, AtomicU32, Ordering},
  RwLock, RwLockReadGuard, RwLockWriteGuard,
};

use crate::{
  disk::{PageRef, PAGE_SIZE},
  utils::ShortenedRwLock,
};

const EVICTION_BIT: u32 = 1 << 31;

pub struct FrameState {
  pin: AtomicU32,
  frame_id: usize,
}
impl FrameState {
  pub fn new(frame_id: usize) -> Self {
    Self {
      pin: AtomicU32::new(EVICTION_BIT),
      frame_id,
    }
  }
  pub fn try_evict(&self) -> bool {
    self
      .pin
      .compare_exchange(0, EVICTION_BIT, Ordering::Release, Ordering::Acquire)
      .is_ok()
  }
  pub fn try_pin(&self) -> bool {
    if EVICTION_BIT & self.pin.fetch_add(1, Ordering::Release) == 0 {
      return true;
    };
    self.pin.fetch_sub(1, Ordering::Release);
    false
  }

  pub fn completion_evict(&self, pin: u32) {
    self.pin.store(pin, Ordering::Release);
  }

  pub fn get_frame_id(&self) -> usize {
    self.frame_id
  }
  pub fn unpin(&self) {
    self.pin.fetch_sub(1, Ordering::Release);
  }
}

pub struct TempFrameState {
  pin: AtomicU32,
  page: RwLock<PageRef<PAGE_SIZE>>,
  dirty: AtomicBool,
}

impl TempFrameState {
  pub fn new(page: PageRef<PAGE_SIZE>) -> Self {
    Self {
      pin: AtomicU32::new(EVICTION_BIT),
      page: RwLock::new(page),
      dirty: AtomicBool::new(false),
    }
  }

  pub fn try_evict(&self) -> bool {
    self
      .pin
      .compare_exchange(1, EVICTION_BIT, Ordering::Release, Ordering::Acquire)
      .is_ok()
  }

  pub fn for_write(&self) -> RwLockWriteGuard<'_, PageRef<PAGE_SIZE>> {
    self.page.wl()
  }
  pub fn for_read(&self) -> RwLockReadGuard<'_, PageRef<PAGE_SIZE>> {
    self.page.rl()
  }
  pub fn mark_dirty(&self) {
    self.dirty.fetch_or(true, Ordering::Release);
  }
  pub fn is_dirty(&self) -> bool {
    self.dirty.load(Ordering::Acquire)
  }

  pub fn try_pin(&self) -> bool {
    if EVICTION_BIT & self.pin.fetch_add(1, Ordering::Release) == 0 {
      return true;
    };
    self.pin.fetch_sub(1, Ordering::Release);
    false
  }

  pub fn completion_evict(&self, pin: u32) {
    self.pin.store(pin, Ordering::Release);
  }

  pub fn unpin(&self) {
    self.pin.fetch_sub(1, Ordering::Release);
  }
}
