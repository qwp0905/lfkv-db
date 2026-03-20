use std::mem::replace;

use super::{CursorNode, DataEntry, Key, LeafNode, Pointer};
use crate::{
  error::{Error, Result},
  transaction::TxOrchestrator,
};

pub struct CursorIterator<'a> {
  tx_id: usize,
  orchestrator: &'a TxOrchestrator,
  leaf: LeafNode,
  pos: usize,
  end: Option<Key>,
  closed: bool,
  committed: &'a bool,
}
impl<'a> CursorIterator<'a> {
  pub fn new(
    tx_id: usize,
    orchestrator: &'a TxOrchestrator,
    leaf: LeafNode,
    pos: usize,
    committed: &'a bool,
    end: Option<Key>,
  ) -> Self {
    Self {
      tx_id,
      orchestrator,
      leaf,
      pos,
      end,
      committed,
      closed: false,
    }
  }

  fn find_value(&self, ptr: Pointer) -> Result<Option<Vec<u8>>> {
    let mut slot = self.orchestrator.fetch(ptr)?.for_read();
    loop {
      let entry = slot.as_ref().deserialize::<DataEntry>()?;
      if let Some(v) = entry.find_value(self.tx_id, |i| self.orchestrator.is_visible(i)) {
        return Ok(Some(v));
      }

      match entry.get_next() {
        Some(i) => drop(replace(&mut slot, self.orchestrator.fetch(i)?.for_read())),
        None => return Ok(None),
      }
    }
  }

  pub fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    if *self.committed {
      return Err(Error::TransactionClosed);
    }

    if self.closed {
      return Ok(None);
    }
    loop {
      for i in self.pos..self.leaf.len() {
        let (key, ptr) = self.leaf.at(i);
        if self.end.as_ref().map(|e| key.ge(e)).unwrap_or(false) {
          self.closed = true;
          return Ok(None);
        }

        self.pos += 1;
        if let Some(value) = self.find_value(*ptr)? {
          return Ok(Some((key.clone(), value)));
        }
      }

      let index = match self.leaf.get_next() {
        Some(i) => i,
        None => {
          self.closed = true;
          return Ok(None);
        }
      };

      self.leaf = self
        .orchestrator
        .fetch(index)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
        .as_leaf()?;
      self.pos = 0;
    }
  }
}
