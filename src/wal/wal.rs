use std::sync::{Arc, Mutex};

use crate::disk::PageSeeker;

pub struct WAL {
  core: Mutex<Core>,
}

impl WAL {}

struct Core {
  disk: Arc<PageSeeker>,
  max_file_size: usize,
  last_index: usize,
  last_transaction: usize,
}
