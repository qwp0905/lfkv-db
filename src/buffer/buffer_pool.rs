use std::sync::{Arc, Mutex};

use crate::{
  disk::PageSeeker, wal::CommitInfo, ContextReceiver, EmptySender, Page, Result,
  StoppableChannel, ThreadPool,
};

use super::LRUCache;

pub struct DataBlock {
  pub log_index: usize,
  pub tx_id: usize,
  pub undo_index: usize,
  pub data: Page,
}

pub struct BufferPool {
  cache: Arc<Mutex<LRUCache<usize, DataBlock>>>,
}

// use super::PageCache;

// pub struct BufferPool {
//   cache: Arc<PageCache>,
//   disk: Arc<PageSeeker>,
//   background: Arc<ThreadPool<Result<()>>>,
//   flush_c: StoppableChannel<Vec<(usize, usize, Page)>>,
// }

// impl BufferPool {
//   fn start_flush(&self, rx: ContextReceiver<Vec<(usize, usize, Page)>>) {
//     let cache = self.cache.clone();
//     let disk = self.disk.clone();
//     self.background.schedule(move || {
//       while let Ok((v, done)) = rx.recv_all() {
//         for (tx_id, i, p) in v {
//           disk.write(i, p)?;
//           cache.flush(tx_id, i);
//         }

//         disk.fsync()?;
//         done.map(|tx| tx.close());
//       }
//       Ok(())
//     })
//   }

//   fn start_commit(&self, rx: ContextReceiver<CommitInfo>) {
//     let cache = self.cache.clone();
//     self.background.schedule(move || {
//       while let Ok(commit) = rx.recv_new() {
//         let indexes = match cache.uncommitted(commit.tx_id) {
//           None => continue,
//           Some(indexes) => indexes,
//         };
//         for index in indexes {}
//         // cache.commit(tx_id);
//       }
//       Ok(())
//     });
//   }
// }
