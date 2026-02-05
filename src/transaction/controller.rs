use std::sync::{Arc, Mutex};

use crate::{
  disk::{DiskController, DiskControllerConfig, PagePool, PAGE_SIZE},
  wal::{LogRecord, WALConfig, WAL},
  BufferPool, Result, ShortenedMutex, ToArc, ToArcMutex,
};

struct TxSnapshot {
  last_tx_id: usize,
  last_commit_id: usize,
  last_log_id: usize,
}

pub struct TxController {
  snapshot: Arc<Mutex<TxSnapshot>>,
  wal: WAL,
  buffer_pool: BufferPool,
  page_pool: Arc<PagePool<PAGE_SIZE>>,
}
impl TxController {
  pub fn open() -> Result<Self> {
    let config = WALConfig {
      path: todo!(),
      max_buffer_size: todo!(),
      checkpoint_interval: todo!(),
      checkpoint_count: todo!(),
      group_commit_delay: todo!(),
      group_commit_count: todo!(),
      max_file_size: todo!(),
    };
    let (wal, last_log_id, last_tx_id, last_commit_id, redo) = WAL::replay(config)?;

    let page_pool = PagePool::new(0).to_arc();
    let disk = DiskController::open(
      DiskControllerConfig {
        path: todo!(),
        read_threads: todo!(),
        write_threads: todo!(),
      },
      page_pool.clone(),
    )?
    .to_arc();
    let mut buffer_pool = BufferPool::new(disk, 1);

    for (tx_id, pages) in redo.drain() {
      for (i, page) in pages {
        // write to buffer pool
        let mut pr = page_pool.acquire(i);
        pr.as_mut().writer().write(page.as_ref());
        buffer_pool.insert(pr);
      }
    }

    let snapshot = TxSnapshot {
      last_commit_id,
      last_log_id,
      last_tx_id,
    }
    .to_arc_mutex();

    Ok(Self {
      snapshot,
      wal,
      buffer_pool,
      page_pool,
    })
  }

  pub fn start_tx(&self) -> Result {
    let record = {
      let mut snap = self.snapshot.l();
      let log_id = snap.last_log_id;
      let tx_id = snap.last_tx_id;
      snap.last_tx_id += 1;
      LogRecord::new_start(log_id, tx_id)
    };
    self.wal.append(record)
  }

  pub fn read() {}

  pub fn write() {}

  pub fn append() {}

  pub fn commit() {}
}
