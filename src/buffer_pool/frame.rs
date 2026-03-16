use std::mem::replace;

use crate::disk::{PageRef, PAGE_SIZE};

pub struct Frame {
  page: PageRef<PAGE_SIZE>,
  /**
   * index can be wrong if nothing allocated.
   * only lru table is the single truth source.
   */
  index: usize,
}
impl Frame {
  #[inline]
  pub fn new(index: usize, page: PageRef<PAGE_SIZE>) -> Self {
    Self { page, index }
  }
  #[inline]
  pub fn empty(page: PageRef<PAGE_SIZE>) -> Self {
    Self::new(0, page)
  }
  #[inline]
  pub fn replace(
    &mut self,
    index: usize,
    page: PageRef<PAGE_SIZE>,
  ) -> PageRef<PAGE_SIZE> {
    self.index = index;
    replace(&mut self.page, page)
  }
  #[inline]
  pub fn get_index(&self) -> usize {
    self.index
  }
  #[inline]
  pub fn page_ref(&self) -> &PageRef<PAGE_SIZE> {
    &self.page
  }
  #[inline]
  pub fn page_ref_mut(&mut self) -> &mut PageRef<PAGE_SIZE> {
    &mut self.page
  }
}
