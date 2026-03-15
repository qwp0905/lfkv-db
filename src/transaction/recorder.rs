use std::sync::Arc;

use crate::{
  buffer_pool::PageSlotWrite,
  error::Result,
  serialize::{Serializable, SerializeFrom},
  wal::WAL,
};

/**
 * Page Recorder
 * only for serialize data into page slot and write slot with byte len into wal.
 * do not implement drop trait to close wal.
 * it has been used for free list, orchestrator, gc.
 */
pub struct PageRecorder {
  wal: Arc<WAL>,
}
impl PageRecorder {
  #[inline]
  pub fn new(wal: Arc<WAL>) -> Self {
    Self { wal }
  }
  #[inline]
  pub fn serialize_and_log<T>(
    &self,
    tx_id: usize,
    slot: &mut PageSlotWrite<'_>,
    data: &T,
  ) -> Result
  where
    T: Serializable,
  {
    let index = slot.get_index();
    let page = slot.as_mut();
    let byte_len = page.serialize_from(data)?;
    self.wal.append_insert(tx_id, index, page.copy_n(byte_len))
  }
}
