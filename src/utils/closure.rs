pub fn first_of_two<T, R>(v: (T, R)) -> T {
  v.0
}
pub fn second_of_two<T, R>(v: (T, R)) -> R {
  v.1
}
pub fn plus_pipe(i: usize) -> impl Fn(usize) -> usize {
  move |v| v + i
}
