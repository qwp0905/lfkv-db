use std::sync::{Arc, Mutex};

use crate::{
  disk::{DiskController, PagePool, PageRef},
  Result, ShortenedMutex,
};

pub struct FreeList<const N: usize> {
  disk: Arc<DiskController<N>>,
  indexes: Arc<Mutex<(usize, Option<PageRef<N>>)>>,
  page_pool: Arc<PagePool<N>>,
}
impl<const N: usize> FreeList<N> {
  pub fn release(&self, mut page: PageRef<N>) -> Result<()> {
    let mut indexes = self.indexes.l();
    page
      .as_mut()
      .writer()
      .write_usize(indexes.1.as_ref().map(|prev| prev.get_index()).unwrap_or(0));
    self.disk.write(&page)?;
    indexes.1 = Some(page);
    Ok(())
  }

  pub fn acquire(&self) -> Result<PageRef<N>> {
    let mut indexes = self.indexes.l();
    match indexes.1.take() {
      Some(last) => {
        let page = self.disk.read(last.as_ref().scanner().read_usize()?)?;
        indexes.1 = page.get_index().ne(&0).then(|| page);
        Ok(last)
      }
      None => {
        let i = indexes.0;
        indexes.0 += 1;
        Ok(self.page_pool.acquire(i))
      }
    }
  }
}
