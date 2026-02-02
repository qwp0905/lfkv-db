use std::{mem::ManuallyDrop, sync::Arc};

use crossbeam::queue::ArrayQueue;

use crate::Page;

pub struct PageRef<const N: usize> {
  page: ManuallyDrop<Page<N>>,
  store: Arc<PageStore<N>>,
  index: usize,
}
impl<const N: usize> PageRef<N> {
  fn with(store: Arc<PageStore<N>>, page: Page<N>, index: usize) -> Self {
    Self {
      page: ManuallyDrop::new(page),
      store,
      index,
    }
  }

  fn new(store: Arc<PageStore<N>>, index: usize) -> Self {
    Self::with(store, Page::new(), index)
  }

  #[inline]
  pub fn get_index(&self) -> usize {
    self.index
  }
}
impl<const N: usize> AsRef<Page<N>> for PageRef<N> {
  #[inline]
  fn as_ref(&self) -> &Page<N> {
    &self.page
  }
}
impl<const N: usize> AsMut<Page<N>> for PageRef<N> {
  #[inline]
  fn as_mut(&mut self) -> &mut Page<N> {
    &mut self.page
  }
}
impl<const N: usize> Drop for PageRef<N> {
  fn drop(&mut self) {
    unsafe {
      if let Ok(_) = self.store.data.push(ManuallyDrop::take(&mut self.page)) {
        return;
      };
      ManuallyDrop::drop(&mut self.page);
    }
  }
}

pub struct PagePool<const N: usize> {
  store: Arc<PageStore<N>>,
}
impl<const N: usize> PagePool<N> {
  pub fn new(cap: usize) -> Self {
    Self {
      store: Arc::new(PageStore::new(cap)),
    }
  }

  pub fn acquire(&self, index: usize) -> PageRef<N> {
    self
      .store
      .as_ref()
      .data
      .pop()
      .map(|p| PageRef::with(self.store.clone(), p, index))
      .unwrap_or_else(|| PageRef::new(self.store.clone(), index))
  }
}

struct PageStore<const N: usize> {
  data: ArrayQueue<Page<N>>,
}
impl<const N: usize> PageStore<N> {
  fn new(cap: usize) -> Self {
    Self {
      data: ArrayQueue::new(cap),
    }
  }
}
