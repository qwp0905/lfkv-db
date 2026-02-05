#[allow(unused)]
#[inline]
pub const fn byte(b: usize) -> usize {
  b
}
#[allow(unused)]
#[inline]
pub const fn kb(b: usize) -> usize {
  (1 << 10) * byte(b)
}
#[allow(unused)]
#[inline]
pub const fn mb(b: usize) -> usize {
  (1 << 10) * kb(b)
}
#[allow(unused)]
#[inline]
pub const fn gb(b: usize) -> usize {
  (1 << 10) * mb(b)
}
#[allow(unused)]
#[inline]
pub const fn tb(b: usize) -> usize {
  (1 << 10) * gb(b)
}
#[allow(unused)]
#[inline]
pub const fn pb(b: usize) -> usize {
  (1 << 10) * tb(b)
}
