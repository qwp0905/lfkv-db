pub fn replace_default<T: Default>(v: &mut T) -> T {
  std::mem::replace(v, Default::default())
}

pub fn deref<T: Copy>(v: &T) -> T {
  *v
}
