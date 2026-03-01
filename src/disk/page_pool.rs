use std::{mem::ManuallyDrop, sync::Arc};

use crossbeam::queue::ArrayQueue;

use super::Page;
use crate::utils::ToArc;

pub struct PageRef<const N: usize> {
  page: ManuallyDrop<Page<N>>,
  store: Arc<PageStore<N>>,
}
impl<const N: usize> PageRef<N> {
  fn from_exists(store: Arc<PageStore<N>>, page: Page<N>) -> Self {
    Self {
      page: ManuallyDrop::new(page),
      store,
    }
  }

  fn new(store: Arc<PageStore<N>>) -> Self {
    Self::from_exists(store, Page::new())
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
      store: PageStore::new(cap).to_arc(),
    }
  }

  pub fn acquire(&self) -> PageRef<N> {
    self
      .store
      .as_ref()
      .data
      .pop()
      .map(|p| PageRef::from_exists(self.store.clone(), p))
      .unwrap_or_else(|| PageRef::new(self.store.clone()))
  }

  #[allow(unused)]
  pub fn len(&self) -> usize {
    self.store.data.len()
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

#[cfg(test)]
mod tests {
  use super::*;

  const PAGE_SIZE: usize = 4 << 10;

  #[test]
  fn test_return_and_reuse() {
    let pool = PagePool::<PAGE_SIZE>::new(10);
    assert_eq!(pool.len(), 0);

    let page = pool.acquire();
    assert_eq!(page.as_ref().as_ref().len(), PAGE_SIZE);

    drop(page);
    assert_eq!(pool.len(), 1);

    let page = pool.acquire();
    assert_eq!(page.as_ref().as_ref().len(), PAGE_SIZE);
    assert_eq!(pool.len(), 0);

    drop(page);
    assert_eq!(pool.len(), 1);
  }

  #[test]
  fn test_drop() {
    let cap = 3;
    let pool = PagePool::<PAGE_SIZE>::new(cap);

    let mut pages = Vec::new();
    for _ in 0..cap {
      pages.push(pool.acquire());
    }
    pages.drain(..).for_each(drop);
    assert_eq!(pool.len(), 3);

    for _ in 0..=cap {
      pages.push(pool.acquire());
    }

    for i in 0..cap {
      drop(pages.pop().unwrap());
      assert_eq!(pool.len(), i + 1);
    }

    drop(pages);
    assert_eq!(pool.len(), 3)
  }
}
