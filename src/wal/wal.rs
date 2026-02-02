use std::sync::Arc;

use crate::disk::RandomWriteDisk;

const WAL_PAGE_SIZE: usize = 1 << 13; // 8192
pub struct WAL {
  disk: Arc<RandomWriteDisk<WAL_PAGE_SIZE>>,
}
