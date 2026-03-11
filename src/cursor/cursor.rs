use std::{marker::PhantomData, mem::replace, sync::Arc};

use super::{
  CursorIterator, CursorNode, DataEntry, InternalNode, NodeFindResult, RecordData,
  TreeHeader, VersionRecord, HEADER_INDEX,
};
use crate::{serialize::SerializeFrom, transaction::TxOrchestrator, Error, Result};

pub struct Cursor {
  orchestrator: Arc<TxOrchestrator>,
  committed: bool,
  tx_id: usize,
  _marker: PhantomData<*const ()>, // do not send to another thread!!!.
}
impl Cursor {
  pub fn initialize(mut self) -> Result {
    let node = CursorNode::initial_state();
    let mut node_slot = self.orchestrator.alloc()?;
    node_slot.as_mut().serialize_from(&node)?;
    self.orchestrator.log(self.tx_id, &node_slot)?;
    let node_index = node_slot.get_index();
    drop(node_slot);

    let root = TreeHeader::new(node_index);
    let mut root_slot = self.orchestrator.fetch(HEADER_INDEX)?.for_write();
    root_slot.as_mut().serialize_from(&root)?;
    self.orchestrator.log(self.tx_id, &root_slot)?;
    drop(root_slot);

    self.commit()
  }
  pub fn new(orchestrator: Arc<TxOrchestrator>, tx_id: usize) -> Self {
    Self {
      orchestrator,
      committed: false,
      tx_id,
      _marker: Default::default(),
    }
  }

  pub fn commit(&mut self) -> Result {
    if self.committed {
      return Err(Error::TransactionClosed);
    }

    self.orchestrator.commit_tx(self.tx_id)?;
    self.committed = true;
    Ok(())
  }

  pub fn abort(&mut self) -> Result {
    if self.committed {
      return Err(Error::TransactionClosed);
    }
    self.orchestrator.abort_tx(self.tx_id)?;
    self.committed = true;
    Ok(())
  }

  fn find_leaf(&self, key: &Vec<u8>) -> Result<usize> {
    let mut index = self
      .orchestrator
      .fetch(HEADER_INDEX)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    while let CursorNode::Internal(node) = self
      .orchestrator
      .fetch(index)?
      .for_read()
      .as_ref()
      .deserialize()?
    {
      index = node.find(key).unwrap_or_else(|i| i);
    }
    Ok(index)
  }

  pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
    if self.committed {
      return Err(Error::TransactionClosed);
    }

    let mut index = self.find_leaf(key)?;
    loop {
      let node = self
        .orchestrator
        .fetch(index)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
        .as_leaf()?;
      match node.find(key) {
        NodeFindResult::Found(_, i) => break index = i,
        NodeFindResult::Move(i) => index = i,
        NodeFindResult::NotFound(_) => return Ok(None),
      }
    }

    let mut slot = self.orchestrator.fetch(index)?.for_read();
    loop {
      let entry: DataEntry = slot.as_ref().deserialize()?;
      for record in entry.get_versions() {
        if record.owner == self.tx_id {
          return Ok(record.data.cloned());
        }

        if record.version > self.tx_id {
          continue;
        }
        if !self.orchestrator.is_visible(&record.owner) {
          continue;
        }

        return Ok(record.data.cloned());
      }

      match entry.get_next() {
        Some(i) => drop(replace(&mut slot, self.orchestrator.fetch(i)?.for_read())),
        None => return Ok(None),
      }
    }
  }

  pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result {
    if self.committed {
      return Err(Error::TransactionClosed);
    }

    let (mut index, mut old_height) = {
      let header = self
        .orchestrator
        .fetch(HEADER_INDEX)?
        .for_read()
        .as_ref()
        .deserialize::<TreeHeader>()?;
      (header.get_root(), header.get_height())
    };
    let mut stack = vec![];

    while let CursorNode::Internal(node) = self
      .orchestrator
      .fetch(index)?
      .for_read()
      .as_ref()
      .deserialize()?
    {
      match node.find(&key) {
        Ok(i) => stack.push(replace(&mut index, i)),
        Err(i) => index = i,
      }
    }

    let (mid_key, right_ptr) = loop {
      let mut leaf_slot = self.orchestrator.fetch(index)?.for_write();
      let mut leaf = leaf_slot.as_ref().deserialize::<CursorNode>()?.as_leaf()?;

      match leaf.find(&key) {
        NodeFindResult::Move(i) => {
          index = i;
          continue;
        }
        NodeFindResult::Found(_, i) => return self.insert_at(i, RecordData::Data(value)),
        NodeFindResult::NotFound(i) => {
          let mut entry_slot = self.orchestrator.alloc()?;
          let entry = DataEntry::init(VersionRecord::new(
            self.tx_id,
            self.orchestrator.current_version(),
            RecordData::Data(value),
          ));
          entry_slot.as_mut().serialize_from(&entry)?;
          self.orchestrator.log(self.tx_id, &entry_slot)?;

          let mut split = match leaf.insert_at(i, key.clone(), entry_slot.get_index()) {
            Some(split) => split,
            None => {
              leaf_slot.as_mut().serialize_from(&CursorNode::Leaf(leaf))?;
              self.orchestrator.log(self.tx_id, &leaf_slot)?;
              return Ok(());
            }
          };

          let mut split_slot = self.orchestrator.alloc()?;
          let mid_key = split.top().clone();
          split.set_prev(leaf_slot.get_index());
          split_slot
            .as_mut()
            .serialize_from(&CursorNode::Leaf(split))?;
          self.orchestrator.log(self.tx_id, &split_slot)?;

          leaf.set_next(split_slot.get_index());
          leaf_slot.as_mut().serialize_from(&CursorNode::Leaf(leaf))?;
          self.orchestrator.log(self.tx_id, &leaf_slot)?;

          break (mid_key, split_slot.get_index());
        }
      }
    };

    let mut split_key = mid_key;
    let mut split_pointer = right_ptr;
    while let Some(index) = stack.pop() {
      match self.apply_split(split_key, split_pointer, index)? {
        Some((k, p)) => {
          split_key = k;
          split_pointer = p;
        }
        None => return Ok(()),
      };
    }

    loop {
      let mut header_latch = self.orchestrator.fetch(HEADER_INDEX)?.for_write();
      let mut header: TreeHeader = header_latch.as_ref().deserialize()?;
      let current_height = header.get_height();
      let mut index = header.get_root();
      if old_height == current_height {
        let mut new_root_latch = self.orchestrator.alloc()?;
        let new_root = InternalNode::initialize(split_key, index, split_pointer);
        new_root_latch
          .as_mut()
          .serialize_from(&CursorNode::Internal(new_root))?;
        self.orchestrator.log(self.tx_id, &new_root_latch)?;
        header.set_root(new_root_latch.get_index());
        drop(new_root_latch);

        header.increase_height();
        header_latch.as_mut().serialize_from(&header)?;
        self.orchestrator.log(self.tx_id, &header_latch)?;
        return Ok(());
      }

      let diff = (current_height - old_height) as usize;
      old_height = current_height;
      drop(header_latch);
      let mut stack = vec![];

      while stack.len() < diff {
        let node = self
          .orchestrator
          .fetch(index)?
          .for_read()
          .as_ref()
          .deserialize::<CursorNode>()?
          .as_internal()?;
        match node.find(&split_key) {
          Ok(i) => stack.push(replace(&mut index, i)),
          Err(i) => index = i,
        }
      }

      while let Some(index) = stack.pop() {
        match self.apply_split(split_key, split_pointer, index)? {
          Some((k, p)) => {
            split_key = k;
            split_pointer = p;
          }
          None => return Ok(()),
        };
      }
    }
  }

  fn apply_split(
    &self,
    key: Vec<u8>,
    ptr: usize,
    current: usize,
  ) -> Result<Option<(Vec<u8>, usize)>> {
    let mut index = current;

    let (mut slot, mut internal) = loop {
      let slot = self.orchestrator.fetch(index)?.for_write();
      let mut internal = slot.as_ref().deserialize::<CursorNode>()?.as_internal()?;
      match internal.insert_or_next(&key, ptr) {
        Ok(_) => break (slot, internal),
        Err(i) => index = i,
      }
    };

    let (split_node, split_key) = match internal.split_if_needed() {
      Some(split) => split,
      None => {
        slot
          .as_mut()
          .serialize_from(&CursorNode::Internal(internal))?;
        self.orchestrator.log(self.tx_id, &slot)?;
        return Ok(None);
      }
    };

    let split_index = {
      let mut split_slot = self.orchestrator.alloc()?;
      internal.set_right(&split_key, split_slot.get_index());
      split_slot
        .as_mut()
        .serialize_from(&CursorNode::Internal(split_node))?;
      self.orchestrator.log(self.tx_id, &split_slot)?;
      split_slot.get_index()
    };

    slot
      .as_mut()
      .serialize_from(&CursorNode::Internal(internal))?;
    self.orchestrator.log(self.tx_id, &slot)?;

    Ok(Some((split_key, split_index)))
  }

  fn insert_at(&self, entry_index: usize, data: RecordData) -> Result {
    let mut slot = self.orchestrator.fetch(entry_index)?.for_write();
    let mut entry: DataEntry = slot.as_ref().deserialize()?;
    if let Some(owner) = entry.get_last_owner() {
      if owner != self.tx_id && self.orchestrator.is_active(&owner) {
        return Err(Error::WriteConflict);
      }
    }

    let version = self.orchestrator.current_version();
    let record = VersionRecord::new(self.tx_id, version, data);

    if entry.is_available(&record) {
      entry.append(record);
      slot.as_mut().serialize_from(&entry)?;
      self.orchestrator.log(self.tx_id, &slot)?;
      return Ok(());
    }

    let mut new_slot = self.orchestrator.alloc()?;
    new_slot.as_mut().serialize_from(&entry)?;
    self.orchestrator.log(self.tx_id, &new_slot)?;

    let mut new_entry = DataEntry::init(record);
    new_entry.set_next(new_slot.get_index());

    slot.as_mut().serialize_from(&new_entry)?;
    self.orchestrator.log(self.tx_id, &slot)?;
    Ok(())
  }

  pub fn remove(&self, key: &Vec<u8>) -> Result {
    if self.committed {
      return Err(Error::TransactionClosed);
    }
    let mut index = self.find_leaf(key)?;
    loop {
      let node = self
        .orchestrator
        .fetch(index)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
        .as_leaf()?;
      match node.find(key) {
        NodeFindResult::Found(_, i) => return self.insert_at(i, RecordData::Tombstone),
        NodeFindResult::Move(i) => index = i,
        NodeFindResult::NotFound(_) => return Ok(()),
      }
    }
  }

  pub fn scan(&self, start: &Vec<u8>, end: &Vec<u8>) -> Result<CursorIterator<'_>> {
    if self.committed {
      return Err(Error::TransactionClosed);
    }

    let mut index = self.find_leaf(start)?;
    let (leaf, pos) = loop {
      let node = self
        .orchestrator
        .fetch(index)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
        .as_leaf()?;
      match node.find(start) {
        NodeFindResult::Found(pos, _) => break (node, pos),
        NodeFindResult::Move(i) => index = i,
        NodeFindResult::NotFound(pos) => break (node, pos),
      }
    };

    Ok(CursorIterator::new(
      self.tx_id,
      &self.orchestrator,
      leaf,
      pos,
      &self.committed,
      Some(end.clone()),
    ))
  }

  pub fn scan_all(&self) -> Result<CursorIterator<'_>> {
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
    let leaf = loop {
      let node: CursorNode = self
        .orchestrator
        .fetch(index)?
        .for_read()
        .as_ref()
        .deserialize()?;
      match node {
        CursorNode::Internal(internal) => index = internal.first_child(),
        CursorNode::Leaf(leaf) => break leaf,
      };
    };

    Ok(CursorIterator::new(
      self.tx_id,
      &self.orchestrator,
      leaf,
      0,
      &self.committed,
      None,
    ))
  }
}
impl Drop for Cursor {
  fn drop(&mut self) {
    if self.committed {
      return;
    }

    let _ = self.abort();
  }
}
