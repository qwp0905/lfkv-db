mod bucket;
use bucket::*;
mod list;
use list::*;
mod lru;
use lru::*;

use std::{mem::replace, sync::Arc};

use crate::{
  disk::{DiskController, PageRef, PAGE_SIZE},
  Bitmap, Result,
};

pub struct BufferPool {
  frame: Vec<PageRef<PAGE_SIZE>>,
  table: LRUTable<usize, usize>,
  dirty: Bitmap,

  disk: Arc<DiskController<PAGE_SIZE>>,
}
impl BufferPool {
  pub fn new(disk: Arc<DiskController<PAGE_SIZE>>, cap: usize) -> Self {
    Self {
      frame: Vec::with_capacity(cap),
      table: LRUTable::new(cap),
      dirty: Bitmap::new(cap),
      disk,
    }
  }

  fn get_frame(&mut self, index: usize) -> Result<usize> {
    if let Some(id) = self.table.get(&index) {
      return Ok(*id);
    };

    let page = self.disk.read(index)?;
    if !self.table.is_full() {
      let id = self.frame.len();
      self.frame.push(page);
      self.table.insert(index, id);
      return Ok(id);
    };

    let (_, evicted) = self.table.evict().unwrap();
    self.table.insert(index, evicted);
    let evcited_page = replace(&mut self.frame[evicted], page);
    if self.dirty.remove(evicted) {
      self.disk.write(&evcited_page)?;
    }
    Ok(evicted)
  }
  pub fn get(&mut self, index: usize) -> Result<&PageRef<PAGE_SIZE>> {
    let id = self.get_frame(index)?;
    Ok(&self.frame[id])
  }
  pub fn get_mut(&mut self, index: usize) -> Result<&mut PageRef<PAGE_SIZE>> {
    let id = self.get_frame(index)?;
    Ok(&mut self.frame[id])
  }

  pub fn insert(&mut self, page: PageRef<PAGE_SIZE>) -> Result<PageRef<PAGE_SIZE>> {
    let id = self.get_frame(page.get_index())?;
    self.dirty.insert(id);
    Ok(replace(&mut self.frame[id], page))
  }

  pub fn flush(&mut self) -> Result {
    for i in self.dirty.iter() {
      self.disk.write(&self.frame[i])?;
    }
    self.disk.fsync()?;
    self.dirty.clear();
    Ok(())
  }
}
