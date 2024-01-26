use std::sync::Arc;

use crate::disk::PageSeeker;

pub struct BTreeIndex {
  disk: Arc<PageSeeker>,
}
