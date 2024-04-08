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

pub struct Link<T>(NonNull<T>);
impl<T> Link<T> {
  pub fn new(v: T) -> Self {
    Self(NonNull::from(Box::leak(Box::new(v))))
  }

  pub fn null() -> Self {
    Self(NonNull::dangling())
  }

  pub fn replace(&mut self, v: T) -> T {
    std::mem::replace(unsafe { self.0.as_mut() }, v)
  }
}

impl<T> AsMut<T> for Link<T> {
  fn as_mut(&mut self) -> &mut T {
    unsafe { self.0.as_mut() }
  }
}

impl<T> AsRef<T> for Link<T> {
  fn as_ref(&self) -> &T {
    unsafe { self.0.as_ref() }
  }
}
