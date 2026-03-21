use std::sync::Arc;

use crate::{
  buffer_pool::WritableSlot,
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
    slot: &mut WritableSlot<'_>,
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

  pub fn log_multi<T, R>(
    &self,
    tx_id: usize,
    slot1: &mut WritableSlot<'_>,
    data1: &T,
    slot2: &mut WritableSlot<'_>,
    data2: &R,
  ) -> Result
  where
    T: Serializable,
    R: Serializable,
  {
    let index1 = slot1.get_index();
    let page = slot1.as_mut();
    let byte_len = page.serialize_from(data1)?;
    let data1 = page.copy_n(byte_len);

    let index2 = slot2.get_index();
    let page = slot2.as_mut();
    let byte_len = page.serialize_from(data2)?;
    let data2 = page.copy_n(byte_len);

    self.wal.append_multi(tx_id, index1, data1, index2, data2)
  }
}
