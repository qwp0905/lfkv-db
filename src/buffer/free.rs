use std::sync::{Arc, Mutex};

use crate::{
  disk::{DiskController, PagePool, PageRef},
  wal::{LogRecord, WAL},
  BufferPool, Result, ShortenedMutex, TxOrchestrator,
};

pub struct FreeList<const N: usize> {
  indexes: Arc<Mutex<(usize, Option<usize>)>>,
  page_pool: Arc<PagePool<N>>,
  tx_controller: Arc<TxOrchestrator>,
}
impl<const N: usize> FreeList<N> {
  pub fn release(&self, mut page: PageRef<N>) -> Result<()> {
    let mut indexes = self.indexes.l();
    page.as_mut().writer().write_usize(indexes.1.unwrap_or(0));
    self.tx_controller.write();
    // self.disk.write(&page)?;
    indexes.1 = Some(page.get_index());
    Ok(())
  }

  pub fn acquire(&self) -> Result<PageRef<N>> {
    let mut indexes = self.indexes.l();
    match indexes.1.take() {
      Some(last) => {
        let page = self.disk.read(last)?;
        let next = page.as_ref().scanner().read_usize()?;
        indexes.1 = next.ne(&0).then(|| next);
        Ok(page)
      }
      None => {
        let i = indexes.0;
        indexes.0 += 1;
        Ok(self.page_pool.acquire(i))
      }
    }
  }
}
