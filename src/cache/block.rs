use std::cell::Cell;

use crate::{
  disk::{Page, PageRef, PendingIO, Pointer, PAGE_SIZE},
  table::TableHandleRef,
  utils::{create_static_ref, AtomicSBox, SBox},
  Result,
};

pub struct BlockFlusher<'a> {
  pages: &'a AtomicSBox<PageRef<PAGE_SIZE>>,
  handle: &'a TableHandleRef,
  pointer: Pointer,
}
impl<'a> BlockFlusher<'a> {
  const fn new(
    pages: &'a AtomicSBox<PageRef<PAGE_SIZE>>,
    handle: &'a TableHandleRef,
    pointer: Pointer,
  ) -> Self {
    Self {
      pages,
      handle,
      pointer,
    }
  }
  pub fn submit(self) -> PendingFlush {
    let page = self.pages.load();

    // SAFETY: `write_async` needs a `'static` page because the IO worker may run
    // after this function returns. `PendingFlush` keeps an `SBox` clone of the
    // loaded page, and `finalize(self)` waits for the async write before that clone
    // is dropped. Therefore the submitted page remains alive until the worker is
    // done with the slice.
    let static_ref = unsafe { create_static_ref::<Page>(&**page) };
    let handle = self.handle.disk().write_async(self.pointer, static_ref);
    PendingFlush {
      handle: Some(handle),
      _page: page,
    }
  }
}

pub struct PendingFlush {
  handle: Option<PendingIO>,
  _page: SBox<PageRef<PAGE_SIZE>>,
}
impl PendingFlush {
  pub fn finalize(mut self) -> Result {
    self.handle.take().unwrap().wait_flatten()
  }
}
impl Drop for PendingFlush {
  fn drop(&mut self) {
    let Some(handle) = self.handle.take() else {
      return;
    };
    let _ = handle.wait();
  }
}

/**
 * Cached page for one table block.
 *
 * The page pointer can be atomically swapped when a new page version is
 * installed. epoch is protected by batch mutation in writable slot.
 */
pub struct CachedBlock {
  page: AtomicSBox<PageRef<PAGE_SIZE>>,
  pointer: Pointer,
  handle: TableHandleRef,
  epoch: Cell<u64>,
}
impl CachedBlock {
  #[inline]
  pub fn new(pointer: Pointer, page: PageRef<PAGE_SIZE>, handle: TableHandleRef) -> Self {
    Self {
      page: AtomicSBox::new(page),
      pointer,
      handle,
      epoch: Cell::new(0),
    }
  }

  pub unsafe fn advance_epoch(&self, page: PageRef<PAGE_SIZE>) {
    self.page.store(page);
    self.epoch.set(self.epoch.get() + 1);
  }

  pub const unsafe fn get_epoch(&self) -> u64 {
    self.epoch.get()
  }

  #[inline]
  pub const fn get_pointer(&self) -> Pointer {
    self.pointer
  }

  #[inline]
  pub fn load_page(&self) -> SBox<PageRef<PAGE_SIZE>> {
    self.page.load()
  }

  #[inline]
  pub const fn handle(&self) -> &TableHandleRef {
    &self.handle
  }

  /**
   * Write the current page to disk.
   */
  pub const fn flusher(&self) -> BlockFlusher<'_> {
    BlockFlusher::new(&self.page, &self.handle, self.pointer)
  }
}

unsafe impl Sync for CachedBlock {}
