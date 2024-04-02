use std::ptr::NonNull;

pub trait Pointer<T> {
  fn from_box(v: T) -> NonNull<T>;
  fn refs(&self) -> &T;
  fn muts(&mut self) -> &mut T;
}
impl<T> Pointer<T> for NonNull<T> {
  fn from_box(v: T) -> NonNull<T> {
    NonNull::from(Box::leak(Box::new(v)))
  }

  fn refs(&self) -> &T {
    unsafe { self.as_ref() }
  }

  fn muts(&mut self) -> &mut T {
    unsafe { self.as_mut() }
  }
}
