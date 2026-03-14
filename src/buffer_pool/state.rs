use crossbeam::{atomic::AtomicCell, utils::Backoff};

#[derive(Clone, Copy, Eq)]
enum Pin {
  Fetched(usize),
  Eviction,
}
impl PartialEq for Pin {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (Pin::Fetched(i), Pin::Fetched(j)) => i == j,
      (Pin::Eviction, Pin::Eviction) => true,
      _ => false,
    }
  }
}

pub struct FrameState {
  pin: AtomicCell<Pin>,
  frame_id: usize,
}
impl FrameState {
  pub fn new(frame_id: usize) -> Self {
    Self {
      pin: AtomicCell::new(Pin::Eviction),
      frame_id,
    }
  }
  pub fn try_evict(&self) -> bool {
    self
      .pin
      .compare_exchange(Pin::Fetched(0), Pin::Eviction)
      .is_ok()
  }
  pub fn try_pin(&self) -> bool {
    let backoff = Backoff::new();
    loop {
      match self.pin.load() {
        Pin::Eviction => return false,
        Pin::Fetched(i) => {
          if self
            .pin
            .compare_exchange(Pin::Fetched(i), Pin::Fetched(i + 1))
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
    self.pin.store(Pin::Fetched(pin))
  }

  pub fn get_frame_id(&self) -> usize {
    self.frame_id
  }
  pub fn unpin(&self) {
    let backoff = Backoff::new();
    loop {
      match self.pin.load() {
        Pin::Fetched(i) => {
          if self
            .pin
            .compare_exchange(Pin::Fetched(i), Pin::Fetched(i - 1))
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
