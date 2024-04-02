use std::ptr::NonNull;

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
