use std::{mem::replace, sync::Arc};

use crate::{
  buffer_pool::{CachedPage, CachedPageWrite},
  cursor::{
    header::{TreeHeader, HEADER_INDEX},
    node::{CursorNode, DataEntry, LeafNode, NodeFindResult},
  },
  transaction::TxOrchestrator,
  Error, Result, Serializable,
};

pub struct Cursor {
  orchestrator: Arc<TxOrchestrator>,
  committed: bool,
  tx_id: usize,
}
impl Cursor {
  pub fn new(orchestrator: Arc<TxOrchestrator>, tx_id: usize) -> Self {
    Self {
      orchestrator,
      committed: false,
      tx_id,
    }
  }

  pub fn initialize(&self) -> Result {
    Ok(())
  }
  pub fn commit(&self) -> Result {
    Ok(())
  }

  pub fn get(&self, key: &Vec<u8>) -> Result<Vec<u8>> {
    if self.committed {
      return Err(Error::TransactionClosed);
    }

    let header: TreeHeader = self
      .orchestrator
      .fetch(HEADER_INDEX)?
      .for_read()
      .as_ref()
      .deserialize()?;

    let mut index = header.get_root();
    loop {
      let node: CursorNode = self
        .orchestrator
        .fetch(index)?
        .for_read()
        .as_ref()
        .deserialize()?;
      match node {
        CursorNode::Internal(internal) => index = internal.find(key),
        CursorNode::Leaf(leaf) => match leaf.find(key) {
          NodeFindResult::Found(i) => break index = i,
          NodeFindResult::Move(i) => index = i,
          NodeFindResult::NotFound(_) => return Err(Error::NotFound),
        },
      }
    }

    loop {
      let entry: DataEntry = self
        .orchestrator
        .fetch(index)?
        .for_read()
        .as_ref()
        .deserialize()?;

      let iter = match entry.find(self.tx_id) {
        Ok(data) => return Ok(data.clone()),
        Err(iter) => iter,
      };
      for (version, data) in iter {
        if self.orchestrator.is_available(*version) {
          return Ok(data.clone());
        }
      }

      match entry.get_next() {
        Some(i) => index = i,
        None => return Err(Error::NotFound),
      }
    }
  }

  pub fn insert(&self, key: Vec<u8>, data: Vec<u8>) -> Result {
    if self.committed {
      return Err(Error::TransactionClosed);
    }
    let header: TreeHeader = self
      .orchestrator
      .fetch(HEADER_INDEX)?
      .for_read()
      .as_ref()
      .deserialize()?;

    let mut index = header.get_root();
    let mut stack = vec![];

    let leaf_index = loop {
      let node: CursorNode = self
        .orchestrator
        .fetch(index)?
        .for_read()
        .as_ref()
        .deserialize()?;
      match node {
        CursorNode::Internal(internal) => {
          stack.push(replace(&mut index, internal.find(&key)))
        }
        CursorNode::Leaf(_) => break index,
      }
    };

    let mut latch: CachedPageWrite<'_> = loop {
      let mut page = self.orchestrator.fetch(index)?.for_write();
      let mut node = page
        .as_ref()
        .deserialize::<CursorNode, Error>()?
        .as_leaf()?;

      break match node.find(&key) {
        NodeFindResult::Move(i) => {
          index = i;
          continue;
        }
        NodeFindResult::Found(i) => self.orchestrator.fetch(i)?.for_write(),
        NodeFindResult::NotFound(i) => {
          let entry = self.orchestrator.alloc()?.for_write();
          node.insert_at(i, key.clone(), entry.get_index());
          CursorNode::Leaf(node).serialize(page.as_mut())?;
          self.orchestrator.log(self.tx_id, page.get_index(), &page)?;
          entry
        }
      };
    };

    let mut entry = loop {
      let mut entry: DataEntry = latch.as_ref().deserialize()?;
      match entry.versions.binary_search_by(|(v, _)| self.tx_id.cmp(v)) {
        Ok(i) => {
          replace(&mut entry.versions[i].1, data);
          break entry;
        }
        Err(i) => {
          if i == entry.versions.len() {
            if let Some(next) = entry.get_next() {
              let _ = replace(&mut latch, self.orchestrator.fetch(next)?.for_write());
              continue;
            }
          }

          let tmp = entry.versions.split_off(i);
          entry.versions.extend_from_slice(&[(self.tx_id, data)]);
          entry.versions.extend(tmp);
          break entry;
        }
      }
    };
    if let Some(split) = entry.is_available() {
      let mut slot = self.orchestrator.alloc()?.for_write();
      split.serialize(slot.as_mut())?;
      self.orchestrator.log(self.tx_id, slot.get_index(), &slot)?;
    }
    entry.serialize(latch.as_mut())?;
    self
      .orchestrator
      .log(self.tx_id, latch.get_index(), &latch)?;
    drop(latch);

    // read leaf and split

    while let Some(index) = stack.pop() {
      // read internal and split
    }
    Ok(())
  }
}
