use std::{collections::VecDeque, mem::replace};

use super::{CursorNode, DataEntry, Key, LeafNode, Pointer};
use crate::{
  buffer_pool::Prefetched,
  error::{Error, Result},
  transaction::TxOrchestrator,
};

const MAX_PREFETCH: usize = 3;

pub struct CursorIterator<'a> {
  tx_id: usize,
  orchestrator: &'a TxOrchestrator,
  leaf: LeafNode,
  pos: usize,
  end: Option<Key>,
  closed: bool,
  committed: &'a bool,
  prefetched: VecDeque<Prefetched>,
  pending: VecDeque<Pointer>,
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
    let mut pending = leaf
      .get_entries()
      .take_while(|(k, _)| end.as_ref().map(|e| e > k).unwrap_or(true))
      .map(|(_, p)| *p)
      .collect::<Vec<_>>();
    if pending.len() == leaf.len() {
      if let Some(ptr) = leaf.get_next() {
        pending.push(ptr)
      }
    }

    let mut iter = Self {
      tx_id,
      orchestrator,
      leaf,
      pos,
      end,
      committed,
      closed: false,
      prefetched: VecDeque::with_capacity(MAX_PREFETCH),
      pending: pending.into_iter().skip(pos).collect(),
    };
    iter.refill();
    iter
  }

  fn refill(&mut self) {
    while self.prefetched.len() < MAX_PREFETCH {
      match self.pending.pop_front() {
        Some(ptr) => self
          .prefetched
          .push_back(self.orchestrator.fetch_async(ptr)),
        None => return,
      }
    }
  }

  fn entry(&self, prefetch: Prefetched) -> Result<Option<Vec<u8>>> {
    let mut slot = prefetch.wait()?.for_read();

    loop {
      let entry = slot.as_ref().deserialize::<DataEntry>()?;
      for record in entry.get_versions() {
        if record.owner == self.tx_id || self.orchestrator.is_visible(&record.owner) {
          return Ok(record.data.cloned());
        }
      }

      match entry.get_next() {
        Some(i) => drop(replace(&mut slot, self.orchestrator.fetch(i)?.for_read())),
        None => return Ok(None),
      }
    }
  }

  pub fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    if self.closed {
      return Ok(None);
    }

    loop {
      if *self.committed {
        return Err(Error::TransactionClosed);
      }

      let prefetched = match self.prefetched.pop_front() {
        Some(v) => v,
        None => break,
      };

      self.refill();

      if self.pos < self.leaf.len() {
        let (key, _) = self.leaf.at(self.pos);
        self.pos += 1;
        if let Some(data) = self.entry(prefetched)? {
          return Ok(Some((key.clone(), data)));
        }
        continue;
      }

      self.pos = 0;
      let leaf = prefetched
        .wait()?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
        .as_leaf()?;

      for (_, ptr) in leaf
        .get_entries()
        .take_while(|(k, _)| self.end.as_ref().map(|e| e > k).unwrap_or(true))
      {
        self.pending.push_back(*ptr);
      }

      if self.pending.len() == leaf.len() {
        if let Some(ptr) = leaf.get_next() {
          self.pending.push_back(ptr)
        }
      }
      self.leaf = leaf;
      self.refill();
    }

    self.closed = true;
    Ok(None)
  }
}

/**
 * must drop earlier then cursor.
 */
impl<'a> Drop for CursorIterator<'a> {
  fn drop(&mut self) {
    while let Some(p) = self.prefetched.pop_front() {
      let _ = p.wait().map(|p| p.release());
    }
  }
}
