use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  ptr::NonNull,
};

pub fn first_of_two<T, R>(v: (T, R)) -> T {
  v.0
}
pub fn second_of_two<T, R>(v: (T, R)) -> R {
  v.1
}
pub fn plus_pipe(i: usize) -> impl Fn(usize) -> usize {
  move |v| v + i
}

pub fn deref<T: Copy>(v: &T) -> T {
  *v
}

pub fn unsafe_ref<'a, K>(v: NonNull<K>) -> &'a K {
  unsafe { v.as_ref() }
}

pub trait Callable<T, R> {
  fn call(&self, v: T) -> R;
}
impl<T, R, F> Callable<T, R> for F
where
  F: Fn(T) -> R,
{
  fn call(&self, v: T) -> R {
    self(v)
  }
}
pub trait CallableMut<T, R> {
  fn call(&mut self, v: T) -> R;
}
impl<T, R, F: FnMut(T) -> R> CallableMut<T, R> for F {
  fn call(&mut self, v: T) -> R {
    self(v)
  }
}
pub trait CallableOnce<T, R> {
  fn call(self, v: T) -> R;
}
impl<T, R, F: FnOnce(T) -> R> CallableOnce<T, R> for F {
  fn call(self, v: T) -> R {
    self(v)
  }
}

pub trait SafeCallable<T, R> {
  type Error;
  fn safe_call(&self, v: T) -> std::result::Result<R, Self::Error>;
}
impl<T, R, F> SafeCallable<T, R> for F
where
  T: UnwindSafe + 'static,
  F: Fn(T) -> R + RefUnwindSafe,
{
  type Error = Box<dyn std::any::Any + Send>;
  fn safe_call(&self, v: T) -> std::result::Result<R, Self::Error> {
    std::panic::catch_unwind(|| self(v))
  }
}
