use std::sync::atomic::{AtomicUsize, Ordering};

use crate::{
  disk::{page_pool::PageRef, RandomWriteDisk},
  Result,
};

pub struct FreeList<const N: usize> {
  disk: RandomWriteDisk<N>,
  last_index: AtomicUsize,
  last_released: AtomicUsize,
}
impl<const N: usize> FreeList<N> {
  pub fn release(&self, mut page: PageRef<N>) -> Result<()> {
    let index = page.get_index();
    loop {
      let prev = self.last_released.load(Ordering::SeqCst);
      page.as_mut().writer().write_usize(prev);
      self.disk.write(page)?;
      if let Ok(_) = self.last_released.compare_exchange(
        prev,
        index,
        Ordering::SeqCst,
        Ordering::SeqCst,
      ) {
        return Ok(());
      };
    }
  }
}
