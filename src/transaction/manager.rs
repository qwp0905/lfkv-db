use std::{
  collections::HashMap,
  panic::{RefUnwindSafe, UnwindSafe},
  sync::{Arc, Mutex},
};

use crossbeam::channel::{Receiver, Sender};
use utils::ShortLocker;

use crate::thread::ThreadPool;

use super::{lock::PageLocker, PageLock};

#[derive(Debug)]
pub struct TransactionManager {
  tree_locks: Arc<Mutex<HashMap<usize, PageLocker>>>,
  background: ThreadPool,
  release: Sender<Option<usize>>,
}
impl TransactionManager {
  fn start_release(&self, rx: Receiver<Option<usize>>) {
    let cloned = Arc::clone(&self.tree_locks);
    self.background.schedule(move || {
      while let Ok(Some(index)) = rx.recv() {
        let mut locks = cloned.l();
        if let Some(pl) = locks.get_mut(&index) {
          let blocked = pl.release();
          if blocked.is_none() {
            continue;
          }
          for tx in blocked.unwrap() {
            tx.send(()).unwrap();
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
      rx.recv().unwrap();
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
      rx.recv().unwrap();
    }
  }
}
impl Drop for TransactionManager {
  fn drop(&mut self) {
    self.release.send(None).unwrap();
  }
}

// impl UnwindSafe for TransactionManager {}
// impl RefUnwindSafe for TransactionManager {}
