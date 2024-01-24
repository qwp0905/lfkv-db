pub fn replace_default<T: Default>(v: &mut T) -> T {
  std::mem::replace(v, Default::default())
}
