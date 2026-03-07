use std::{
  ptr::copy_nonoverlapping,
  sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
};

use super::{FsyncResult, WALSegment, WAL_BLOCK_SIZE};
use crate::{
  disk::PageRef,
  error::Result,
  utils::{UnsafeBorrow, UnsafeTake},
};

const U16_MASK: u32 = 0xFFFF;

pub struct LogBuffer {
  offset: AtomicU64, // entry pin (24bit) + offest (40bit)
  entry: PageRef<WAL_BLOCK_SIZE>,
  commit_count: AtomicU32,
  index: usize,
  segment_pin: *const AtomicUsize,
  segment: *const WALSegment,
}
impl LogBuffer {
  const BIT: u64 = 40;
  const MASK: u64 = (1 << Self::BIT) - 1;
  const OFFSET_BYTE: u64 = 2;

  pub fn new(
    entry: PageRef<WAL_BLOCK_SIZE>,
    index: usize,
    segment_pin: *const AtomicUsize,
    segment: *const WALSegment,
  ) -> Self {
    Self {
      offset: AtomicU64::new(Self::OFFSET_BYTE),
      entry,
      commit_count: AtomicU32::new(0),
      index,
      segment_pin,
      segment,
    }
  }

  pub fn pin_segment(&self) {
    unsafe { &*self.segment_pin }.fetch_add(1, Ordering::Release);
  }
  pub fn pin_entry(&self, len: usize) -> (usize, u32) {
    let prev = self.offset.fetch_add(
      ((len as u64) & Self::MASK) | (1 << Self::BIT),
      Ordering::Release,
    );
    ((prev & Self::MASK) as usize, (prev >> Self::BIT) as u32)
  }
  pub fn apply_entry_len(&self, len: u32) {
    self.write_at(&((len & U16_MASK) as u16).to_le_bytes(), 0)
  }
  pub fn write_at(&self, record: &[u8], offset: usize) {
    let ptr = self.entry.as_ref().as_ptr() as *mut u8;
    let len = record.len();
    unsafe { copy_nonoverlapping(record.as_ptr(), ptr.add(offset), len) };
  }
  pub fn load_commit(&self) -> u32 {
    self.commit_count.load(Ordering::Acquire)
  }
  pub fn flush(&self) -> Result<FsyncResult> {
    let segment = self.segment.borrow_unsafe();
    segment.write(self.index, &self.entry)?;
    Ok(segment.fsync())
  }
  pub fn write_to_disk(&self) -> Result {
    self.segment.borrow_unsafe().write(self.index, &self.entry)
  }
  pub fn commit_entry(&self) {
    self.commit_count.fetch_add(1, Ordering::Release);
  }
  pub fn unpin_segment(&self) {
    self
      .segment_pin
      .borrow_unsafe()
      .fetch_sub(1, Ordering::Release);
  }
  pub fn get_index(&self) -> usize {
    self.index
  }
  pub fn copy_segment(&self) -> (*const WALSegment, *const AtomicUsize) {
    (self.segment, self.segment_pin)
  }
  pub fn take_segement(&self) -> (WALSegment, AtomicUsize) {
    (self.segment.take_unsafe(), self.segment_pin.take_unsafe())
  }
  pub fn load_offset(&self) -> usize {
    (self.offset.load(Ordering::Acquire) & Self::MASK) as usize
  }
  pub fn load_segment_pinned(&self) -> usize {
    self.segment_pin.borrow_unsafe().load(Ordering::Acquire)
  }
}

unsafe impl Send for LogBuffer {}
unsafe impl Sync for LogBuffer {}
