use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub trait ShortLocker<T: ?Sized> {
  fn l(&self) -> MutexGuard<'_, T>;
}
impl<T: ?Sized> ShortLocker<T> for Mutex<T> {
  fn l(&self) -> MutexGuard<'_, T> {
    self.lock().unwrap()
  }
}

pub trait ShortRwLocker<T: ?Sized> {
  fn wl(&self) -> RwLockWriteGuard<'_, T>;
  fn rl(&self) -> RwLockReadGuard<'_, T>;
}
impl<T: ?Sized> ShortRwLocker<T> for RwLock<T> {
  fn rl(&self) -> RwLockReadGuard<'_, T> {
    self.read().unwrap()
  }
  fn wl(&self) -> RwLockWriteGuard<'_, T> {
    self.write().unwrap()
  }
}
