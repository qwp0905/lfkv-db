use std::fs::File;

use crate::size;

static WAL_PAGE_SIZE: usize = size::kb(32);

pub struct LogReadWriter {
  offset: usize,
  file: File,
}
