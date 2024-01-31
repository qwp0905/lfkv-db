use std::{
  collections::HashMap,
  sync::{Arc, Mutex, RwLock},
  time::{Duration, Instant},
};

use crate::{
  disk::PageSeeker, ContextReceiver, Error, Page, Result, Serializable, ShortenedMutex,
  ShortenedRwLock, StoppableChannel, ThreadPool, PAGE_SIZE,
};

use super::LRUCache;

const UNDO_PAGE_SIZE: usize = PAGE_SIZE + 24;

pub struct UndoLog {
  log_index: usize,
  tx_id: usize,
  data: Page,
  undo_index: usize,
}

impl UndoLog {
  pub fn new(log_index: usize, tx_id: usize, data: Page, undo_index: usize) -> Self {
    Self {
      log_index,
      tx_id,
      data,
      undo_index,
    }
  }
}
impl Clone for UndoLog {
  fn clone(&self) -> Self {
    Self::new(
      self.log_index,
      self.tx_id,
      self.data.copy(),
      self.undo_index,
    )
  }
}
impl Serializable<Error, UNDO_PAGE_SIZE> for UndoLog {
  fn serialize(&self) -> core::result::Result<Page<UNDO_PAGE_SIZE>, Error> {
    let mut page = Page::new();
    let mut wt = page.writer();
    wt.write(&self.log_index.to_be_bytes())?;
    wt.write(&self.tx_id.to_be_bytes())?;
    wt.write(&self.undo_index.to_be_bytes())?;
    wt.write(self.data.as_ref())?;

    Ok(page)
  }
  fn deserialize(value: &Page<UNDO_PAGE_SIZE>) -> core::result::Result<Self, Error> {
    let mut sc = value.scanner();
    let log_index = sc.read_usize()?;
    let tx_id = sc.read_usize()?;
    let undo_index = sc.read_usize()?;
    let data = sc.read_n(PAGE_SIZE)?.into();

    Ok(UndoLog::new(log_index, tx_id, data, undo_index))
  }
}

pub struct RollbackStorage {
  cache: Arc<Mutex<LRUCache<usize, UndoLog>>>,
  disk: Arc<PageSeeker<UNDO_PAGE_SIZE>>,
  cursor: Arc<RwLock<usize>>,
  background: ThreadPool<Result<()>>,
  delay: Duration,
  count: usize,
  io_c: StoppableChannel<Page<UNDO_PAGE_SIZE>, usize>,
}
impl RollbackStorage {
  fn start_io(&self, rx: ContextReceiver<Page<UNDO_PAGE_SIZE>, usize>) {
    let count = self.count;
    let delay = self.delay;
    let disk = self.disk.clone();
    let cursor = self.cursor.clone();

    self.background.schedule(move || {
      let mut start = Instant::now();
      let mut point = delay;
      let mut m = HashMap::new();
      while let Ok(o) = rx.recv_done_or_timeout(delay) {
        if let Some((p, done)) = o {
          let mut c = cursor.wl();
          m.insert(*c, done);
          disk.write(*c, p)?;
          *c += 1;

          if m.len() < count {
            point -= Instant::now().duration_since(start).min(point);
            start = Instant::now();
            continue;
          }
        }

        if m.len() != 0 {
          disk.fsync()?;
          m.drain().for_each(|(i, done)| done.send(i).unwrap())
        }

        point = delay;
        start = Instant::now();
      }
      Ok(())
    });
  }

  pub fn get(&self, log_index: usize, undo_index: usize) -> Result<Page> {
    if let Some(log) = self.cache.l().get(&undo_index) {
      if log_index <= log.log_index {
        return Ok(log.data.copy());
      }

      return self.get(log_index, log.undo_index);
    }

    let log: UndoLog = self.disk.read(log_index)?.deserialize()?;
    self.cache.l().insert(log_index, log.clone());
    if log_index <= log.log_index {
      return Ok(log.data);
    }

    return self.get(log_index, log.undo_index);
  }

  pub fn append(&mut self, log: UndoLog) -> Result<usize> {
    Ok(self.io_c.send_with_done(log.serialize()?).recv().unwrap())
  }
}
