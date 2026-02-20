use std::panic::{RefUnwindSafe, UnwindSafe};

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

  #[inline]
  fn safe_call(&self, v: T) -> std::result::Result<R, Self::Error> {
    std::panic::catch_unwind(|| self(v))
  }
}
