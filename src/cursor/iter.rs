use super::{CursorNode, DataEntry, Key, LeafNode, RecordData};
use crate::{error::Result, transaction::TxOrchestrator};

pub struct CursorIterator<'a> {
  tx_id: usize,
  orchestrator: &'a TxOrchestrator,
  leaf: LeafNode,
  pos: usize,
  end: Option<Key>,
  closed: bool,
}
impl<'a> CursorIterator<'a> {
  pub fn new(
    tx_id: usize,
    orchestrator: &'a TxOrchestrator,
    leaf: LeafNode,
    pos: usize,
    end: Option<Key>,
  ) -> Self {
    Self {
      tx_id,
      orchestrator,
      leaf,
      pos,
      end,
      closed: false,
    }
  }

  pub fn try_next(&mut self) -> Result<Option<Vec<u8>>> {
    if self.closed {
      return Ok(None);
    }
    loop {
      'leaf: for i in self.pos..self.leaf.len() {
        let (key, ptr) = self.leaf.at(i);
        if self.end.as_ref().map(|e| key.ge(e)).unwrap_or(false) {
          self.closed = true;
          return Ok(None);
        }

        let mut index = *ptr;
        loop {
          let entry = self
            .orchestrator
            .fetch(index)?
            .for_read()
            .as_ref()
            .deserialize::<DataEntry>()?;

          for record in entry.find(self.tx_id) {
            if record.owner == self.tx_id || self.orchestrator.is_visible(&record.owner) {
              match &record.data {
                RecordData::Data(data) => {
                  self.pos = i + 1;
                  return Ok(Some(data.clone()));
                }
                RecordData::Tombstone => continue 'leaf,
              }
            }
          }

          match entry.get_next() {
            Some(i) => index = i,
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
