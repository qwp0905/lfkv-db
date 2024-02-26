use std::{
  collections::BTreeMap,
  sync::{Arc, Mutex},
};

use crate::{
  disk::Finder, size, wal::CommitInfo, ContextReceiver, Error, Page, Result,
  Serializable, ShortenedMutex, PAGE_SIZE,
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
  disk: Arc<Finder<BLOCK_SIZE>>,
}
impl BufferPool {
  fn start_write(&self, rx: ContextReceiver<(usize, Page<BLOCK_SIZE>), Result>) {
    let disk = self.disk.clone();
    rx.to_done("bufferpool write", 1, move |(index, page)| {
      disk.write(index, page)
    });
  }

  fn start_flush(&self, rx: ContextReceiver<(), Option<usize>>) {
    let cache = self.cache.clone();
    let disk = self.disk.clone();
    rx.to_all("bufferpool flush", 1, move |_| {
      let max_index = match cache.flush_all() {
        Ok(o) => o,
        Err(_) => return None,
      };
      if let Err(_) = disk.fsync() {
        return None;
      }

      max_index
    });
  }

  fn start_commit(&self, rx: ContextReceiver<CommitInfo, Result>) {
    let uncommitted = self.uncommitted.clone();
    let cache = self.cache.clone();
    let disk = self.disk.clone();
    let rollback = self.rollback.clone();

    rx.to_new("bufferpool commit", 1, move |commit| {
      let mut u = uncommitted.l();
      if let Some(v) = u.remove(&commit.tx_id) {
        for index in v {
          let undo_index = match cache.commit(index, commit.as_ref()) {
            Ok(applied) => {
              if applied {
                continue;
              }

              let mut block: DataBlock = disk.read_to(index)?;
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
      Ok(())
    });
  }

  // fn start_rollback(&self, rx: ContextReceiver<usize>) {
  //   let uncommitted = self.uncommitted.clone();
  //   let cache = self.cache.clone();

  //   rx.to_done("bufferpool rollback", 1, move |tx_id| {
  //     if let Some(v) = uncommitted.l().get(&tx_id) {
  //       for i in v {}
  //     }
  //   });
  // }

  pub fn get(&self, commit_index: usize, index: usize) -> Result<Page> {
    let block = {
      match self.cache.get(&index) {
        Some(block) => block.copy(),
        None => {
          let block: DataBlock = self.disk.read_to(index)?;
          self.cache.insert(index, block.copy());
          block
        }
      }
    };

    if block.commit_index >= commit_index {
      return Ok(block.data.copy());
    }

    self.rollback.get(commit_index, block.undo_index)
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
    self.cache.insert_new(index, new_block);
    self.uncommitted.l().entry(tx_id).or_default().push(index);
    Ok(())
  }
}
