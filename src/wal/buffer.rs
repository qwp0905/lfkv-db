use std::{
  ptr::copy_nonoverlapping,
  sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
};

use super::{FsyncResult, WALSegment, WAL_BLOCK_SIZE};
use crate::{disk::PageRef, error::Result};

const U32_MASK: u64 = 0xFFFF_FFFF;
const U16_MASK: u32 = 0xFFFF_FFFF;

pub struct LogBuffer {
  offset: AtomicU64,
  entry: PageRef<WAL_BLOCK_SIZE>,
  commit_count: AtomicU32,
  index: usize,
  segment_pin: *mut AtomicUsize,
  segment: *mut WALSegment,
}
impl LogBuffer {
  pub fn new(
    entry: PageRef<WAL_BLOCK_SIZE>,
    index: usize,
    segment_pin: *mut AtomicUsize,
    segment: *mut WALSegment,
  ) -> Self {
    Self {
      offset: AtomicU64::new(2),
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
    let prev = self
      .offset
      .fetch_add(((len as u64) & U32_MASK) | (1 << 32), Ordering::Release);
    ((prev & U32_MASK) as usize, (prev >> 32) as u32)
  }
  pub fn apply_entry_len(&self, len: u32) {
    self.write_at(&((len & U16_MASK) as u16).to_be_bytes(), 0)
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
    let segment = unsafe { &*self.segment };
    segment.write(self.index, &self.entry)?;
    Ok(segment.fsync())
  }
  pub fn write_to_disk(&self) -> Result {
    unsafe { &*self.segment }.write(self.index, &self.entry)
  }
  pub fn commit_entry(&self) {
    self.commit_count.fetch_add(1, Ordering::Release);
  }
  pub fn unpin_segment(&self) {
    unsafe { &*self.segment_pin }.fetch_sub(1, Ordering::Release);
  }
  pub fn get_index(&self) -> usize {
    self.index
  }
  pub fn copy_segment(&self) -> (*mut WALSegment, *mut AtomicUsize) {
    (self.segment, self.segment_pin)
  }
  pub fn take_segement(&self) -> (WALSegment, AtomicUsize) {
    unsafe {
      (
        *Box::from_raw(self.segment),
        *Box::from_raw(self.segment_pin),
      )
    }
  }
  pub fn load_offset(&self) -> usize {
    (self.offset.load(Ordering::Acquire) & U32_MASK) as usize
  }
  pub fn load_segment_pinned(&self) -> usize {
    unsafe { &*self.segment_pin }.load(Ordering::Acquire)
  }
}

unsafe impl Send for LogBuffer {}
unsafe impl Sync for LogBuffer {}
