use std::sync::{Arc, Mutex, RwLock};

pub trait ToArc {
  fn to_arc(self) -> Arc<Self>;
}
impl<T> ToArc for T {
  #[inline]
  fn to_arc(self) -> Arc<Self> {
    Arc::new(self)
  }
}

pub trait ToArcMutex {
  fn to_arc_mutex(self) -> Arc<Mutex<Self>>;
}
impl<T> ToArcMutex for T {
  #[inline]
  fn to_arc_mutex(self) -> Arc<Mutex<Self>> {
    Mutex::new(self).to_arc()
  }
}

pub trait ToArcRwLock {
  fn to_arc_rwlock(self) -> Arc<RwLock<Self>>;
}
impl<T> ToArcRwLock for T {
  fn to_arc_rwlock(self) -> Arc<RwLock<Self>> {
    RwLock::new(self).to_arc()
  }
}
