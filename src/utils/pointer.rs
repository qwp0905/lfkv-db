use std::sync::Arc;

pub trait ToArc {
  fn to_arc(self) -> Arc<Self>;
}
impl<T> ToArc for T {
  #[inline(always)]
  fn to_arc(self) -> Arc<Self> {
    Arc::new(self)
  }
}

pub trait ToRawPointer {
  fn to_raw_ptr(self) -> *const Self;
}
impl<T> ToRawPointer for T {
  #[inline(always)]
  fn to_raw_ptr(self) -> *const Self {
    Box::into_raw(Box::new(self))
  }
}

pub trait UnsafeBorrow<T> {
  fn borrow_unsafe(self) -> &'static T;
}
impl<T> UnsafeBorrow<T> for *const T {
  #[inline(always)]
  fn borrow_unsafe(self) -> &'static T {
    unsafe { &*self }
  }
}
pub trait UnsafeTake<T> {
  fn take_unsafe(self) -> T;
}
impl<T> UnsafeTake<T> for *const T {
  #[inline(always)]
  fn take_unsafe(self) -> T {
    unsafe { *Box::from_raw(self as *mut T) }
  }
}
