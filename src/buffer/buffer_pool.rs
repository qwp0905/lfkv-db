use std::{
  collections::HashMap,
  sync::{Arc, Mutex, RwLock},
};

use crate::{
  disk::PageSeeker, ContextReceiver, EmptySender, Error, Page, Result,
  ShortenedMutex, ShortenedRwLock, StoppableChannel, ThreadPool,
};

use super::{Cache, MVCC};

pub struct BufferPool {
  cache: Arc<Mutex<Cache<usize, MVCC>>>,
  evicted: Arc<RwLock<HashMap<usize, MVCC>>>,
  max_cache_size: usize,
  disk: Arc<PageSeeker>,
  background: ThreadPool<Result<()>>,
  flush_c: StoppableChannel<Vec<(usize, usize, Page)>>,
}

impl BufferPool {
  fn start_flush(&self, rx: ContextReceiver<Vec<(usize, usize, Page)>>) {
    let cache = self.cache.clone();
    let disk = self.disk.clone();
    let evicted = self.evicted.clone();
    self.background.schedule(move || {
      while let Ok((v, done)) = rx.recv_all() {
        for (tx_id, i, p) in v {
          disk.write(i, p)?;

          let mut c = cache.l();
          if let Some(mvcc) = c.get_mut(&i) {
            mvcc.split_off(tx_id); // include current tx
          }

          let mut ev = evicted.wl();
          if let Some(mvcc) = ev.get_mut(&i) {
            mvcc.split_off(tx_id + 1); // exclude current tx
            if mvcc.is_empty() {
              ev.remove(&i);
            }
          }
        }

        disk.fsync()?;
        done.map(|tx| tx.close());
      }
      Ok(())
    })
  }

  pub fn get(&self, tx_id: usize, index: usize) -> Result<Page> {
    let mut cache = self.cache.l();
    if let Some(v) = cache.get(&index) {
      if let Some(page) = v.view(tx_id) {
        return Ok(page.copy());
      }
    }

    if let Some(v) = self.evicted.rl().get(&index) {
      if let Some(page) = v.view(tx_id) {
        return Ok(page.copy());
      }
    }

    let page = self.disk.read(index)?;
    if page.is_empty() {
      return Err(Error::NotFound);
    }

    cache
      .entry(index)
      .and_modify(|mvcc| mvcc.append(tx_id, page.copy()))
      .or_insert(MVCC::new(vec![(tx_id, page.copy())]));
    if cache.len() >= self.max_cache_size {
      if let Some((i, mvcc)) = cache.pop_old() {
        self.evicted.wl().insert(i, mvcc);
      };
    }

    return Ok(page);
  }

  pub fn insert(&self, tx_id: usize, index: usize, page: Page) -> Result<()> {
    let mut cache = self.cache.l();
    cache
      .entry(index)
      .and_modify(|mvcc| mvcc.append(tx_id, page.copy()))
      .or_insert(MVCC::new(vec![(tx_id, page)]));

    if cache.len() >= self.max_cache_size {
      if let Some((i, mvcc)) = cache.pop_old() {
        self.evicted.wl().insert(i, mvcc);
      };
    }

    Ok(())
  }
}
