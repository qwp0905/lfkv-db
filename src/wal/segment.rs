use super::WAL_BLOCK_SIZE;
use crate::{disk::DiskController, error::Result};

pub struct WALSegment(DiskController<WAL_BLOCK_SIZE>);
impl WALSegment {
  pub fn new(disk: DiskController<WAL_BLOCK_SIZE>) -> Self {
    Self(disk)
  }

  pub fn flush(&self) -> Result {
    self.0.fsync()
  }

  pub fn truncate(self) -> Result {
    self.0.close();
    self.0.unlink()
  }
}
