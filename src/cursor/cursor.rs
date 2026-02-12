use std::{collections::VecDeque, mem::replace, sync::Arc};

use crate::{
  buffer_pool::PageSlotWrite,
  cursor::{
    CursorNode, DataEntry, InternalNode, NodeFindResult, RecordData, TreeHeader,
    VersionRecord, HEADER_INDEX,
  },
  serialize::SerializeFrom,
  transaction::TxOrchestrator,
  Error, Result,
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
        CursorNode::Internal(internal) => match internal.find(key) {
          Ok(i) => index = i,
          Err(i) => index = i,
        },
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

      for record in entry.find(self.tx_id) {
        if record.owner == self.tx_id || self.orchestrator.is_visible(&record.owner) {
          match &record.data {
            RecordData::Data(data) => return Ok(data.clone()),
            RecordData::Tombstone => return Err(Error::NotFound),
          }
        }
      }

      match entry.get_next() {
        Some(i) => index = i,
        None => return Err(Error::NotFound),
      }
    }
  }

  fn alloc_entry(
    &self,
    key: &Vec<u8>,
    create: bool,
  ) -> Result<(PageSlotWrite<'_>, DataEntry)> {
    let header_slot = self.orchestrator.fetch(HEADER_INDEX)?;
    let mut header: TreeHeader = header_slot.for_read().as_ref().deserialize()?;

    let mut index = header.get_root();
    let mut stack = vec![];

    loop {
      let node: CursorNode = self
        .orchestrator
        .fetch(index)?
        .for_read()
        .as_ref()
        .deserialize()?;
      match node {
        CursorNode::Internal(internal) => match internal.find(key) {
          Ok(i) => stack.push(replace(&mut index, i)),
          Err(i) => index = i,
        },
        CursorNode::Leaf(_) => break,
      }
    }

    let (latch, entry, mid_key, right_ptr): (
      PageSlotWrite<'_>,
      DataEntry,
      Vec<u8>,
      usize,
    ) = loop {
      let mut slot = self.orchestrator.fetch(index)?.for_write();
      let mut leaf = slot.as_ref().deserialize::<CursorNode>()?.as_leaf()?;

      match leaf.find(&key) {
        NodeFindResult::Move(i) => {
          index = i;
          continue;
        }
        NodeFindResult::Found(i) => {
          let latch = self.orchestrator.fetch(i)?.for_write();
          let entry = latch.as_ref().deserialize()?;
          return Ok((latch, entry));
        }
        NodeFindResult::NotFound(i) => {
          if !create {
            return Err(Error::NotFound);
          }
          let latch = self.orchestrator.alloc()?.for_write();
          let mut split = match leaf.insert_at(i, key.clone(), latch.get_index()) {
            Some(s) => s,
            None => {
              slot.as_mut().serialize_from(&CursorNode::Leaf(leaf))?;
              self.orchestrator.log(self.tx_id, &slot)?;
              return Ok((latch, DataEntry::new()));
            }
          };

          let mut split_slot = self.orchestrator.alloc()?.for_write();
          split.set_prev(slot.get_index());
          leaf.set_next(split_slot.get_index());
          slot.as_mut().serialize_from(&CursorNode::Leaf(leaf))?;
          self.orchestrator.log(self.tx_id, &slot)?;

          let mid_key = split.top().clone();
          split_slot
            .as_mut()
            .serialize_from(&CursorNode::Leaf(split))?;
          self.orchestrator.log(self.tx_id, &split_slot)?;
          break (latch, DataEntry::new(), mid_key, split_slot.get_index());
        }
      };
    };

    let mut split_key = mid_key;
    let mut split_pointer = right_ptr;
    while let Some(mut index) = stack.pop() {
      let (mut slot, mut internal) = loop {
        let slot = self.orchestrator.fetch(index)?.for_write();
        let mut internal = slot.as_ref().deserialize::<CursorNode>()?.as_internal()?;
        match internal.insert_or_next(&split_key, split_pointer) {
          Ok(_) => break (slot, internal),
          Err(i) => index = i,
        }
      };
      let (split_node, key) = match internal.split_if_needed() {
        Some(split) => split,
        None => {
          slot
            .as_mut()
            .serialize_from(&CursorNode::Internal(internal))?;
          return Ok((latch, entry));
        }
      };

      let split_index = {
        let mut split_slot = self.orchestrator.alloc()?.for_write();
        internal.set_right(&key, split_slot.get_index());
        split_slot
          .as_mut()
          .serialize_from(&CursorNode::Internal(split_node))?;
        split_slot.get_index()
      };

      slot
        .as_mut()
        .serialize_from(&CursorNode::Internal(internal))?;

      split_key = key;
      split_pointer = split_index;
    }

    let new_root_index = {
      let mut new_root_page = self.orchestrator.alloc()?.for_write();
      let new_root =
        InternalNode::inialialize(split_key, header.get_root(), split_pointer);
      new_root_page
        .as_mut()
        .serialize_from(&CursorNode::Internal(new_root))?;
      Ok(new_root_page.get_index())
    }?;

    header.set_root(new_root_index);
    let mut header_latch = header_slot.for_write();
    header_latch.as_mut().serialize_from(&header)?;
    self.orchestrator.log(self.tx_id, &header_latch)?;
    Ok((latch, entry))
  }

  fn write_at(&mut self, key: &Vec<u8>, data: RecordData, create: bool) -> Result {
    if self.committed {
      return Err(Error::TransactionClosed);
    }

    let (mut latch, mut entry) = self.alloc_entry(key, create)?;
    if let Some(owner) = entry.get_last_owner() {
      if owner != self.tx_id && self.orchestrator.is_active(&owner) {
        drop(latch);
        self.abort()?;
        return Err(Error::WriteConflict);
      }
    }

    let version = self.orchestrator.current_version();
    let record = VersionRecord::new(self.tx_id, version, data);
    entry.append(record);

    let mut splitted: Option<VecDeque<VersionRecord>> = None;
    loop {
      if let Some(versions) = splitted.take() {
        entry.apply_split(versions);
      }

      let next = match entry.split_if_full() {
        Some(versions) => {
          splitted = Some(versions);
          match entry.get_next() {
            Some(i) => {
              let split_page = self.orchestrator.fetch(i)?.for_write();
              let split_entry = split_page.as_ref().deserialize()?;
              Some((split_page, split_entry))
            }
            None => {
              let split_page = self.orchestrator.alloc()?.for_write();
              entry.set_next(split_page.get_index());
              Some((split_page, DataEntry::new()))
            }
          }
        }
        None => None,
      };

      latch.as_mut().serialize_from(&entry)?;
      self.orchestrator.log(self.tx_id, &latch)?;

      match next {
        Some((next_latch, next_entry)) => {
          let _ = replace(&mut latch, next_latch);
          entry = next_entry;
        }
        None => break,
      }
    }

    Ok(())
  }

  pub fn insert(&mut self, key: Vec<u8>, data: Vec<u8>) -> Result {
    self.write_at(&key, RecordData::Data(data), true)
  }

  pub fn remove(&mut self, key: &Vec<u8>) -> Result {
    self.write_at(key, RecordData::Tombstone, false)
  }
}
