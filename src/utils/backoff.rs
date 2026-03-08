use std::{
  cell::Cell,
  hint::spin_loop,
  thread::{sleep, yield_now},
  time::Duration,
};

const THREAD_PARK_TIMEOUT: Duration = Duration::from_micros(100);
const YIELD_LIMIT: u8 = 10;
const SPIN_LIMIT: u8 = 6;

pub struct Backoff {
  step: Cell<u8>,
}
impl Backoff {
  pub fn new() -> Self {
    Self { step: Cell::new(0) }
  }

  pub fn reset(&self) {
    self.step.set(0);
  }

  pub fn spin(&self) {
    let step = self.step.get();
    for _ in 0..(1 << step.min(SPIN_LIMIT)) {
      spin_loop();
    }
    if step <= SPIN_LIMIT {
      self.step.set(step + 1);
    }
  }

  pub fn snooze(&self) {
    let step = self.step.get();
    if step > YIELD_LIMIT {
      return sleep(THREAD_PARK_TIMEOUT);
    }
    self.step.set(step + 1);

    if step > SPIN_LIMIT {
      return yield_now();
    }
    for _ in 0..(1 << step) {
      spin_loop();
    }
  }
}
