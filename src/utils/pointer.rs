use std::{
  ops::Deref,
  ptr::NonNull,
  sync::{Arc, Mutex, RwLock},
};

pub trait Pointer<T> {
  fn from_box(v: T) -> Self;
  fn borrow(&self) -> &T;
  fn borrow_mut(&mut self) -> &mut T;
  fn drop(self);
}
impl<T> Pointer<T> for NonNull<T> {
  #[inline]
  fn from_box(v: T) -> Self {
    NonNull::from(Box::leak(Box::new(v)))
  }

  #[inline]
  fn borrow(&self) -> &T {
    unsafe { self.as_ref() }
  }

  #[inline]
  fn borrow_mut(&mut self) -> &mut T {
    unsafe { self.as_mut() }
  }

  #[inline]
  fn drop(self) {
    unsafe { self.drop_in_place() };
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
impl<T> Clone for Link<T> {
  fn clone(&self) -> Self {
    Self(self.0)
  }
}
impl<T> Deref for Link<T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    unsafe { self.0.as_ref() }
  }
}

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
