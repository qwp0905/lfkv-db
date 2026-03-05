use std::sync::Arc;

pub trait ToArc {
  fn to_arc(self) -> Arc<Self>;
}
impl<T> ToArc for T {
  #[inline]
  fn to_arc(self) -> Arc<Self> {
    Arc::new(self)
  }
}

#[allow(unused)]
pub trait ToRawPointer {
  fn to_raw_ptr(self) -> *mut Self;
}
impl<T> ToRawPointer for T {
  fn to_raw_ptr(self) -> *mut Self {
    Box::into_raw(Box::new(self))
  }
}
