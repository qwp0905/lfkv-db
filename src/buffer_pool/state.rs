use std::sync::{
  atomic::{AtomicBool, Ordering},
  RwLock, RwLockReadGuard, RwLockWriteGuard,
};

use crossbeam::{atomic::AtomicCell, utils::Backoff};

use crate::{
  disk::{PageRef, PAGE_SIZE},
  utils::ShortenedRwLock,
};

#[derive(Clone, Copy, Eq)]
pub enum PinCount {
  Fetched(usize),
  Eviction,
}
impl PartialEq for PinCount {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (PinCount::Fetched(i), PinCount::Fetched(j)) => i == j,
      (PinCount::Eviction, PinCount::Eviction) => true,
      _ => false,
    }
  }
}

pub struct FrameState {
  pin: AtomicCell<PinCount>,
  frame_id: usize,
}
impl FrameState {
  pub fn new(frame_id: usize) -> Self {
    Self {
      pin: AtomicCell::new(PinCount::Eviction),
      frame_id,
    }
  }
  pub fn try_evict(&self) -> bool {
    self
      .pin
      .compare_exchange(PinCount::Fetched(0), PinCount::Eviction)
      .is_ok()
  }
  pub fn try_pin(&self) -> bool {
    let backoff = Backoff::new();
    loop {
      match self.pin.load() {
        PinCount::Eviction => return false,
        PinCount::Fetched(i) => {
          if self
            .pin
            .compare_exchange(PinCount::Fetched(i), PinCount::Fetched(i + 1))
            .is_ok()
          {
            return true;
          }

          backoff.spin()
        }
      }
    }
  }

  pub fn completion_evict(&self, pin: usize) {
    self.pin.store(PinCount::Fetched(pin))
  }

  pub fn get_frame_id(&self) -> usize {
    self.frame_id
  }
  pub fn unpin(&self) {
    let backoff = Backoff::new();
    loop {
      match self.pin.load() {
        PinCount::Fetched(i) => {
          if self
            .pin
            .compare_exchange(PinCount::Fetched(i), PinCount::Fetched(i - 1))
            .is_ok()
          {
            return;
          }
        }
        _ => {}
      }

      backoff.spin()
    }
  }
}

pub struct TempFrameState {
  pin: AtomicCell<PinCount>,
  page: RwLock<PageRef<PAGE_SIZE>>,
  dirty: AtomicBool,
}

impl TempFrameState {
  pub fn new(page: PageRef<PAGE_SIZE>) -> Self {
    Self {
      pin: AtomicCell::new(PinCount::Eviction),
      page: RwLock::new(page),
      dirty: AtomicBool::new(false),
    }
  }

  pub fn try_evict(&self) -> bool {
    self
      .pin
      .compare_exchange(PinCount::Fetched(1), PinCount::Eviction)
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
    let backoff = Backoff::new();
    loop {
      match self.pin.load() {
        PinCount::Eviction => return false,
        PinCount::Fetched(i) => {
          if self
            .pin
            .compare_exchange(PinCount::Fetched(i), PinCount::Fetched(i + 1))
            .is_ok()
          {
            return true;
          }

          backoff.spin()
        }
      }
    }
  }

  pub fn completion_evict(&self, pin: usize) {
    self.pin.store(PinCount::Fetched(pin))
  }

  pub fn unpin(&self) {
    let backoff = Backoff::new();
    loop {
      match self.pin.load() {
        PinCount::Fetched(i) => {
          if self
            .pin
            .compare_exchange(PinCount::Fetched(i), PinCount::Fetched(i - 1))
            .is_ok()
          {
            return;
          }
        }
        _ => {}
      }

      backoff.spin()
    }
  }
}
