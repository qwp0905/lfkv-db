use std::{
  collections::HashMap,
  sync::{Arc, Mutex},
};

use utils::{size, DroppableReceiver, EmptySender, ShortLocker};

use crate::thread::{ContextReceiver, StoppableChannel, ThreadPool};

use super::{PageLock, PageLocker};

pub struct TransactionManager {
  tree_locks: Arc<Mutex<HashMap<usize, PageLocker>>>,
  background: ThreadPool,
  release: StoppableChannel<usize>,
}
impl TransactionManager {
  pub fn new() -> Self {
    let (channel, recv) = StoppableChannel::new();
    let tm = Self {
      tree_locks: Default::default(),
      background: ThreadPool::new(1, size::kb(2), "transaction_manager", None),
      release: channel,
    };
    tm.start_release(recv);
    return tm;
  }

  fn start_release(&self, rx: ContextReceiver<usize>) {
    let cloned = Arc::clone(&self.tree_locks);
    self.background.schedule(move || {
      while let Ok(index) = rx.recv_new() {
        let mut locks = cloned.l();
        if let Some(pl) = locks.get_mut(&index) {
          if let Some(blocked) = pl.release() {
            for tx in blocked {
              tx.close();
            }
            continue;
          }
          locks.remove(&index);
        }
      }
    })
  }

  pub fn fetch_read(&self, index: usize) -> PageLock {
    loop {
      let rx = {
        let mut tree_locks = self.tree_locks.l();
        match tree_locks.get_mut(&index) {
          Some(tr) => {
            let releaser = self.release.to_owned();
            match tr.fetch_read(index, releaser) {
              Ok(r) => return r,
              Err(rx) => rx,
            }
          }
          None => {
            tree_locks.insert(index, PageLocker::new());
            continue;
          }
        }
      };
      rx.drop_one();
    }
  }

  pub fn fetch_write(&self, index: usize) -> PageLock {
    loop {
      let rx = {
        let mut tree_locks = self.tree_locks.l();
        match tree_locks.get_mut(&index) {
          Some(tr) => {
            let releaser = self.release.to_owned();
            match tr.fetch_write(index, releaser) {
              Ok(r) => return r,
              Err(rx) => rx,
            }
          }
          None => {
            tree_locks.insert(index, PageLocker::new());
            continue;
          }
        }
      };
      rx.drop_one();
    }
  }
}
impl Drop for TransactionManager {
  fn drop(&mut self) {
    self.release.terminate();
  }
}
