use std::mem::replace;

use super::{CursorNode, DataEntry, Key, LeafNode};
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

  pub fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    if *self.committed {
      return Err(Error::TransactionClosed);
    }

    if self.closed {
      return Ok(None);
    }
    loop {
      'leaf: for i in self.pos..self.leaf.len() {
        let (key, ptr) = self.leaf.at(i);
        if self.end.as_ref().map(|e| key >= e).unwrap_or(false) {
          self.closed = true;
          return Ok(None);
        }

        let mut read_latch = self.orchestrator.fetch(*ptr)?.for_read();
        loop {
          let entry = read_latch.as_ref().deserialize::<DataEntry>()?;

          for record in entry.get_versions() {
            if record.owner == self.tx_id || self.orchestrator.is_visible(&record.owner) {
              match record.data.cloned() {
                Some(data) => {
                  self.pos = i + 1;
                  return Ok(Some((key.clone(), data)));
                }
                None => continue 'leaf,
              }
            }
          }

          match entry.get_next() {
            Some(i) => drop(replace(
              &mut read_latch,
              self.orchestrator.fetch(i)?.for_read(),
            )),
            None => continue 'leaf,
          }
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
