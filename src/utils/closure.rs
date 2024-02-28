pub fn plus_pipe(i: usize) -> impl Fn(usize) -> usize {
  move |v| v + i
}
