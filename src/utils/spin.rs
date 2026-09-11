use std::{cell::Cell, hint::spin_loop};

const MAX: u8 = 5;
pub struct SpinBackoff(Cell<u8>);
impl SpinBackoff {
  pub const fn new() -> Self {
    Self(Cell::new(0))
  }
  pub const fn is_completed(&self) -> bool {
    self.0.get() >= MAX
  }
  pub fn spin(&self) {
    let current = self.0.get();
    spin(1 << current);
    self.0.set((current + 1).min(MAX));
  }
  pub fn reset(&self) {
    self.0.set(0);
  }
}

fn spin(n: usize) {
  for _ in 0..n {
    spin_loop();
  }
}
