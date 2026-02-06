use std::{
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  time::Duration,
};

use crate::{
  buffer_pool::BufferPoolConfig,
  disk::{PageRef, PAGE_SIZE},
  wal::{LogRecord, WALConfig, WAL},
  BufferPool, CachedPage, Error, Result, SingleWorkThread, ToArc, WorkBuilder,
};

struct TxStatus {
  last_tx_id: AtomicUsize,
  last_log_id: Arc<AtomicUsize>,
  last_free: AtomicUsize,
}

pub struct TxOrchestrator {
  status: TxStatus,
  wal: Arc<WAL>,
  buffer_pool: Arc<BufferPool>,
  checkpoint: SingleWorkThread<(), Result>,
}
impl TxOrchestrator {
  pub fn new(
    buffer_pool_config: BufferPoolConfig,
    wal_config: WALConfig,
  ) -> Result<Self> {
    let buffer_pool = BufferPool::open(buffer_pool_config)?.to_arc();
    let (wal, last_log_id, last_tx_id, redo) = WAL::replay(wal_config)?;
    let wal = wal.to_arc();
    let last_log_id = AtomicUsize::new(last_log_id).to_arc();
    let status = TxStatus {
      last_tx_id: AtomicUsize::new(last_tx_id),
      last_log_id: last_log_id.clone(),
      last_free: AtomicUsize::new(last_tx_id),
    };
    for (_, i, page) in redo {
      buffer_pool
        .read(i)?
        .as_mut()
        .as_mut()
        .writer()
        .write(page.as_ref());
    }

    let b = buffer_pool.clone();
    let w = wal.clone();
    let checkpoint = WorkBuilder::new()
      .name("")
      .stack_size(1)
      .single()
      .with_timeout(Duration::new(1, 1), move |_| {
        let log_id = last_log_id.fetch_add(1, Ordering::SeqCst);
        b.flush()?;
        match w.append(LogRecord::new_checkpoint(log_id, 0)) {
          Ok(_) => {}
          Err(Error::WALCapacityExceeded) => {}
          Err(err) => return Err(err),
        };
        w.flush()
      });

    Ok(Self {
      checkpoint,
      status,
      wal,
      buffer_pool,
    })
  }
  pub fn fetch(&self, index: usize) -> Result<CachedPage<'_>> {
    self.buffer_pool.read(index)
  }
  pub fn log<P: AsRef<PageRef<PAGE_SIZE>>>(&self, tx_id: usize, page: &P) -> Result {
    let log_id = self.status.last_log_id.fetch_add(1, Ordering::SeqCst);
    let page = page.as_ref();
    let record =
      LogRecord::new_insert(log_id, tx_id, page.get_index(), page.as_ref().copy());
    match self.wal.append(record) {
      Ok(_) => return Ok(()),
      Err(Error::WALCapacityExceeded) => self.checkpoint.send_await(()),
      Err(err) => return Err(err),
    };
    Ok(())
  }

  pub fn alloc(&self) -> Result<CachedPage<'_>> {
    loop {
      let f = self.status.last_free.load(Ordering::SeqCst);
      let page = self.buffer_pool.read(f)?;
      let next = page.as_ref().as_ref().scanner().read_usize()?;
      if let Ok(_) = self.status.last_free.compare_exchange(
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
      .write_usize(self.status.last_free.swap(f, Ordering::SeqCst));
  }

  pub fn start_tx(&self) -> Result<usize> {
    let tx_id = self.status.last_tx_id.fetch_add(1, Ordering::SeqCst);
    let log_id = self.status.last_log_id.fetch_add(1, Ordering::SeqCst);
    let record = LogRecord::new_start(tx_id, log_id);
    match self.wal.append(record) {
      Ok(_) => return Ok(tx_id),
      Err(Error::WALCapacityExceeded) => self.checkpoint.send_await(()),
      Err(err) => return Err(err),
    };
    Ok(tx_id)
  }
}
