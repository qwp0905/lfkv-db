use std::sync::Arc;

use crate::{buffer::BufferPool, transaction::TransactionManager};

mod node;

pub struct Cursor {
  buffer: Arc<BufferPool>,
  transactions: Arc<TransactionManager>,
}
