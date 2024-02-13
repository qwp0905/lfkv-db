use std::{
  collections::{BTreeMap, BTreeSet},
  sync::{Arc, Mutex},
};

use crate::{
  disk::PageSeeker, size, wal::CommitInfo, ContextReceiver, Drain, EmptySender, Error,
  Page, Result, Serializable, ShortenedMutex, ThreadPool, UnwrappedSender, PAGE_SIZE,
};

use super::{CacheStorage, RollbackStorage};

pub const BLOCK_SIZE: usize = size::kb(4);

pub struct DataBlock {
  pub commit_index: usize,
  pub tx_id: usize,
  pub undo_index: usize,
  pub data: Page,
}

impl DataBlock {
  pub fn uncommitted(tx_id: usize, undo_index: usize, data: Page) -> Self {
    Self::new(0, tx_id, undo_index, data)
  }

  pub fn new(commit_index: usize, tx_id: usize, undo_index: usize, data: Page) -> Self {
    Self {
      commit_index,
      tx_id,
      undo_index,
      data,
    }
  }

  pub fn copy(&self) -> Self {
    Self::new(
      self.commit_index,
      self.tx_id,
      self.undo_index,
      self.data.copy(),
    )
  }
}
impl Serializable<Error, BLOCK_SIZE> for DataBlock {
  fn serialize(&self) -> std::prelude::v1::Result<Page<BLOCK_SIZE>, Error> {
    let mut page = Page::new();
    let mut wt = page.writer();
    wt.write(self.commit_index.to_be_bytes().as_ref())?;
    wt.write(self.tx_id.to_be_bytes().as_ref())?;
    wt.write(self.undo_index.to_be_bytes().as_ref())?;
    wt.write(self.data.as_ref())?;
    Ok(page)
  }
  fn deserialize(value: &Page<BLOCK_SIZE>) -> std::prelude::v1::Result<Self, Error> {
    let mut sc = value.scanner();
    let commit_index = sc.read_usize()?;
    let tx_id = sc.read_usize()?;
    let undo_index = sc.read_usize()?;
    let data = sc.read_n(PAGE_SIZE)?.into();
    Ok(Self::new(commit_index, tx_id, undo_index, data))
  }
}

pub struct BufferPool {
  cache: Arc<CacheStorage>,
  rollback: Arc<RollbackStorage>,
  uncommitted: Arc<Mutex<BTreeMap<usize, Vec<usize>>>>,
  dirty: Arc<Mutex<BTreeSet<usize>>>,
  disk: Arc<PageSeeker<BLOCK_SIZE>>,
  background: ThreadPool<Result>,
}
impl BufferPool {
  fn start_flush(&self, rx: ContextReceiver<(), usize>) {
    let cache = self.cache.clone();
    let disk = self.disk.clone();
    let dirty = self.dirty.clone();

    self.background.schedule(move || {
      while let Ok((_, done)) = rx.recv_all() {
        let indexes = dirty.l().drain();
        let mut max_index = 0;
        for i in indexes {
          if let Some(block) = cache.get(&i) {
            disk.write(i, block.serialize()?)?;
            cache.flush(i);
            max_index = block.commit_index.max(max_index);
            continue;
          }
        }

        disk.fsync()?;
        done.map(|tx| tx.must_send(max_index));
      }

      Ok(())
    });
  }

  fn start_commit(&self, rx: ContextReceiver<CommitInfo>) {
    let uncommitted = self.uncommitted.clone();
    let cache = self.cache.clone();
    let disk = self.disk.clone();
    let rollback = self.rollback.clone();

    self.background.schedule(move || {
      while let Ok(commit) = rx.recv_new() {
        let mut u = uncommitted.l();
        if let Some(v) = u.remove(&commit.tx_id) {
          for index in v {
            let undo_index = match cache.commit(index, commit.as_ref()) {
              Ok(effected) => {
                if effected {
                  continue;
                }

                let mut block: DataBlock = disk.read(index)?.deserialize()?;
                if block.tx_id == commit.tx_id {
                  block.commit_index = commit.commit_index;
                  cache.insert(index, block);
                  continue;
                }

                let undo_index = block.undo_index;
                cache.insert(index, block);
                undo_index
              }
              Err(undo_index) => undo_index,
            };
            rollback.commit(undo_index, commit.as_ref())?;
          }
        };
      }

      Ok(())
    });
  }

  pub fn get(&self, commit_index: usize, index: usize) -> Result<Page> {
    let block = {
      match self.cache.get(&index) {
        Some(block) => block.copy(),
        None => {
          let block: DataBlock = self.disk.read(index)?.deserialize()?;
          self.cache.insert(index, block.copy());
          block
        }
      }
    };

    if block.commit_index >= commit_index {
      return Ok(block.data.copy());
    }

    return self.rollback.get(commit_index, block.undo_index);
  }

  pub fn insert(&self, tx_id: usize, index: usize, data: Page) -> Result<()> {
    let block = {
      match self.cache.get(&index) {
        Some(block) => block.copy(),
        None => match self.disk.read(index) {
          Ok(page) => page,
          Err(Error::NotFound) => Page::new_empty(),
          Err(err) => return Err(err),
        }
        .deserialize()?,
      }
    };
    let undo_index = self.rollback.append(block.into())?;
    let new_block = DataBlock::uncommitted(tx_id, undo_index, data);
    self.cache.insert(index, new_block);
    self.dirty.l().insert(index);
    let mut un_c = self.uncommitted.l();
    un_c.entry(tx_id).or_default().push(index);
    Ok(())
  }
}
