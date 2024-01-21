pub fn first_of_two<T, R>(v: (T, R)) -> T {
  v.0
}
pub fn second_of_two<T, R>(v: (T, R)) -> R {
  v.1
}
