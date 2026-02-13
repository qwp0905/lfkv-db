use std::{collections::HashSet, sync::Arc};

use super::{CursorNode, Pointer, TreeHeader, HEADER_INDEX};
use crate::{error::Result, thread::SingleWorkThread, transaction::TxOrchestrator};

pub struct GarbageCollector {
  orchestrator: Arc<TxOrchestrator>,
  entry: Arc<SingleWorkThread<Pointer, Result>>,
}
impl GarbageCollector {
  pub fn run(&self) -> Result {
    let mut index = Some(self.get_leading_leaf()?);

    while let Some(i) = index.take() {
      let mut leaf = self
        .orchestrator
        .fetch(i)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
        .as_leaf()?;

      let mut empty = HashSet::new();
      let mut released = vec![];
      for (key, ptr) in leaf.get_entries() {
        if self.clean_entry(*ptr)? {
          empty.insert(key);
          released.push(*ptr);
        }
      }

      // remove keys from leaf
      // latch and update leaf page

      if !leaf.is_empy() {
        index = leaf.get_next();
        continue;
      }

      let next = match leaf.get_next() {
        Some(i) => i,
        None => break,
      };

      // shrink leaf

      // release entries

      // bottom up shrink toward root
    }
    Ok(())
  }

  fn clean_entry(&self, pointer: Pointer) -> Result<bool> {
    // find entry and remove unusable record
    Ok(true)
  }

  fn get_leading_leaf(&self) -> Result<usize> {
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
        CursorNode::Internal(node) => index = node.first_child(),
        CursorNode::Leaf(_) => break,
      }
    }
    Ok(index)
  }
}
