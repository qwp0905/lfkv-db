use std::{
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  time::Duration,
};

use crate::{
  disk::{PageRef, PAGE_SIZE},
  wal::WAL,
  BufferPool, CachedPage, Error, Result, SingleWorkThread, ToArc, WorkBuilder,
};

struct TxSnapshot {
  last_tx_id: AtomicUsize,
  last_log_id: Arc<AtomicUsize>,
  last_free: AtomicUsize,
}

pub struct TxOrchestrator {
  snapshot: TxSnapshot,
  wal: Arc<WAL>,
  buffer_pool: Arc<BufferPool>,
  checkpoint: SingleWorkThread<(), Result>,
}
impl TxOrchestrator {
  pub fn new() -> Result<Self> {
    let buffer_pool = BufferPool::open()?.to_arc();
    let (wal, last_log_id, last_tx_id, committed) = WAL::replay(config)?;
    let wal = wal.to_arc();
    let last_log_id = AtomicUsize::new(last_log_id).to_arc();
    let snapshot = TxSnapshot {
      last_tx_id: AtomicUsize::new(last_tx_id),
      last_log_id: last_log_id.clone(),
      last_free: AtomicUsize::new(last_tx_id),
    };
    let b = buffer_pool.clone();
    let w = wal.clone();
    let checkpoint = WorkBuilder::new()
      .name("")
      .stack_size(1)
      .single()
      .with_timeout(Duration::new(1, 1), |v| {
        let log_id = last_log_id.fetch_add(1, Ordering::SeqCst);
        b.flush()?;
        w.append_checkpoint(log_id)
      });

    Ok(Self {
      checkpoint,
      snapshot,
      wal,
      buffer_pool,
    })
  }
  pub fn fetch(&self, index: usize) -> Result<CachedPage<'_>> {
    self.buffer_pool.read(index)
  }
  pub fn log<P: AsRef<PageRef<PAGE_SIZE>>>(&self, tx_id: usize, page: &P) -> Result {
    let log_id = self.snapshot.last_log_id.fetch_add(1, Ordering::SeqCst);
    if let Err(Error::WALCapacityExceeded) =
      self.wal.append_insert(tx_id, log_id, page.as_ref())
    {
      self.checkpoint.send_await(());
    };
    Ok(())
  }

  pub fn alloc(&self) -> Result<CachedPage<'_>> {
    loop {
      let f = self.snapshot.last_free.load(Ordering::SeqCst);
      let page = self.buffer_pool.read(f)?;
      let next = page.as_ref().as_ref().scanner().read_usize()?;
      if let Ok(_) = self.snapshot.last_free.compare_exchange(
        f,
        next,
        Ordering::SeqCst,
        Ordering::SeqCst,
      ) {
        return Ok(page);
      }
    }
  }
  pub fn release(&self, mut page: CachedPage<'_>) {
    let f = page.as_ref().get_index();
    page
      .as_mut()
      .as_mut()
      .writer()
      .write_usize(self.snapshot.last_free.swap(f, Ordering::SeqCst));
  }
}
