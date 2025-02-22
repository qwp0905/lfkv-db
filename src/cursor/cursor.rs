use std::{
  ops::Add,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
};

use crate::{
  buffer::{BufferPool, BLOCK_SIZE},
  disk::FreeList,
  logger,
  wal::WriteAheadLog,
  Error, Page, Result, Serializable,
};

use super::{
  entry::MIN_NODE_LEN, CursorEntry, CursorWriter, InternalNode, LeafNode, TreeHeader,
  HEADER_INDEX, MAX_NODE_LEN,
};

pub struct Cursor {
  committed: Arc<AtomicBool>,
  writer: CursorWriter,
}
impl Cursor {
  pub fn new(
    freelist: Arc<FreeList<BLOCK_SIZE>>,
    wal: Arc<WriteAheadLog>,
    buffer: Arc<BufferPool>,
  ) -> Result<Self> {
    let (tx_id, last_commit_index) = wal.new_transaction()?;
    logger::info(format!(
      "cursor id {} and lsn {} init",
      tx_id, last_commit_index
    ));
    Ok(Self {
      committed: Arc::new(AtomicBool::new(false)),
      writer: CursorWriter::new(tx_id, last_commit_index, wal, buffer, freelist),
    })
  }

  pub fn initialize(&self) -> Result {
    if let Err(Error::NotFound) = self
      .writer
      .get(HEADER_INDEX)?
      .deserialize::<TreeHeader, Error>()
    {
      logger::info("there are no tree header and will be initialized");
      let header = TreeHeader::initial_state();
      let root = header.get_root();
      self.writer.update(HEADER_INDEX, header.serialize()?)?;
      self
        .writer
        .update(root, CursorEntry::Leaf(LeafNode::empty()).serialize()?)?;
    };
    Ok(())
  }

  pub fn get(&self, key: &Vec<u8>) -> Result<Vec<u8>> {
    if self.committed.load(Ordering::SeqCst) {
      return Err(Error::TransactionClosed);
    }

    Ok(self.writer.get(self.get_index(key)?)?.into())
  }

  pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result {
    if self.committed.load(Ordering::SeqCst) {
      return Err(Error::TransactionClosed);
    }

    let mut header: TreeHeader = self.writer.get(HEADER_INDEX)?.deserialize()?;
    let (_, evicted, inserted) = self._insert(header.get_root(), key, value.into())?;
    if !inserted {
      return Ok(());
    }

    let (ni, s) = match evicted {
      Some((ni, s)) => (ni, s),
      None => return Ok(()),
    };

    let new_root = CursorEntry::Internal(InternalNode {
      keys: vec![s],
      children: vec![header.get_root(), ni],
    });

    let nri = self.writer.insert(new_root.serialize()?)?;
    header.set_root(nri);
    self.writer.update(HEADER_INDEX, header.serialize()?)?;

    Ok(())
  }

  fn _insert(
    &self,
    current: usize,
    key: Vec<u8>,
    page: Page,
  ) -> Result<(Option<Vec<u8>>, Option<(usize, Vec<u8>)>, bool)> {
    let entry: CursorEntry = self.writer.get(current)?.deserialize()?;
    match entry {
      CursorEntry::Internal(mut node) => {
        let (i, ci) = match node.keys.binary_search(&key) {
          Ok(i) => (i, node.children[i + 1]),
          Err(i) => (i, node.children[i]),
        };

        let (fk, evicted, inserted) = self._insert(ci, key, page)?;
        if !inserted {
          return Ok((None, None, false));
        }

        if let Some(k) = fk {
          node.keys[i] = k;
        }

        if let Some((en, ek)) = evicted {
          node.keys.insert(i, ek);
          node.children.insert(i + 1, en);
          if node.len().le(&MAX_NODE_LEN) {
            self.writer.update(current, node.serialize()?)?;
            return Ok((None, None, true));
          }

          let (n, s) = node.split();
          let ni = self.writer.insert(n.serialize()?)?;
          self.writer.update(current, node.serialize()?)?;
          return Ok((None, Some((ni, s)), true));
        }

        return Ok((None, None, true));
      }
      CursorEntry::Leaf(mut node) => {
        match node.keys.binary_search_by(|(k, _)| k.cmp(&key)) {
          Ok(i) => {
            self.writer.update(node.keys[i].1, page)?;
            return Ok((None, None, false));
          }
          Err(i) => {
            let pi = self.writer.insert(page)?;
            node.keys.insert(i, (key.clone(), pi));
            let fk = (i == 0).then(|| key);

            if node.len().le(&MAX_NODE_LEN) {
              self.writer.update(current, node.serialize()?)?;
              return Ok((fk, None, true));
            }

            let (n, s) = node.split(current);
            let ni = self.writer.insert(n.serialize()?)?;
            node.set_next(ni);
            self.writer.update(current, node.serialize()?)?;
            return Ok((fk, Some((ni, s)), true));
          }
        };
      }
    }
  }

  pub fn delete(&self, key: &Vec<u8>) -> Result<bool> {
    if self.committed.load(Ordering::SeqCst) {
      return Err(Error::TransactionClosed);
    }

    let mut header: TreeHeader = self.writer.get(HEADER_INDEX)?.deserialize()?;
    let root = header.get_root();
    let (e, _) = match self._delete(key, root)? {
      None => return Ok(false),
      Some(v) => v,
    };
    match &e {
      CursorEntry::Leaf(_) => {
        self.writer.update(root, e.serialize()?)?;
        Ok(true)
      }
      CursorEntry::Internal(node) => {
        if node.len().gt(&0) {
          self.writer.update(root, e.serialize()?)?;
          return Ok(true);
        }
        header.set_root(node.children[0]);
        self.writer.update(HEADER_INDEX, header.serialize()?)?;
        self.writer.release(root)?;
        Ok(true)
      }
    }
  }

  fn _delete(
    &self,
    key: &Vec<u8>,
    current: usize,
  ) -> Result<Option<(CursorEntry, Vec<u8>)>> {
    let entry: CursorEntry = self.writer.get(current)?.deserialize()?;
    match entry {
      CursorEntry::Internal(mut node) => {
        let ((ii, ci), left, right) = node.find_family(key);
        let (child_entry, successor) = match self._delete(key, ci)? {
          None => return Ok(None),
          Some(c) => c,
        };
        match child_entry {
          CursorEntry::Internal(mut child) => {
            if node.keys[ii].eq(key) {
              node.keys[ii] = successor.clone();
            }
            if child.len().ge(&MIN_NODE_LEN) {
              self.writer.update(ci, child.serialize()?)?;
              return Ok(Some((CursorEntry::Internal(node), successor)));
            };

            if let Some(left) = left {
              let mut left_entry: CursorEntry = self.writer.get(left)?.deserialize()?;
              if left_entry.as_internal().len().gt(&MIN_NODE_LEN) {
                let (k, p) = left_entry.as_internal().pop_back().unwrap();
                node.push_front(k.clone(), p);
                node.keys[ii] = k;
                self.writer.update(left, left_entry.serialize()?)?;
                self.writer.update(ci, child.serialize()?)?;
                return Ok(Some((CursorEntry::Internal(node), successor)));
              }
            }

            if let Some(right) = right {
              let mut right_entry: CursorEntry = self.writer.get(right)?.deserialize()?;
              if right_entry.as_internal().len().gt(&MIN_NODE_LEN) {
                let (k, p) = right_entry.as_internal().pop_front().unwrap();
                node.push_back(k.clone(), p);
                node.keys[ii.add(1)] = k;
                self.writer.update(right, right_entry.serialize()?)?;
                self.writer.update(ci, child.serialize()?)?;
                return Ok(Some((CursorEntry::Internal(node), successor)));
              }
            }

            if let Some(left) = left {
              let mut left_entry: CursorEntry = self.writer.get(left)?.deserialize()?;
              left_entry
                .as_internal()
                .merge(node.keys.remove(ii), &mut child);
              node.children.remove(ii.add(1));
              self.writer.update(left, left_entry.serialize()?)?;
              self.writer.release(ci)?;
              return Ok(Some((CursorEntry::Internal(node), successor)));
            }

            if let Some(right) = right {
              let mut right_entry: CursorEntry = self.writer.get(right)?.deserialize()?;
              child.merge(
                right_entry.as_internal().keys.remove(0),
                &mut right_entry.as_internal(),
              );
              node.children.remove(ii.add(1));
              self.writer.update(ci, child.serialize()?)?;
              self.writer.release(right)?;
              return Ok(Some((CursorEntry::Internal(node), successor)));
            }

            unreachable!()
          }
          CursorEntry::Leaf(mut child) => {
            if child.len().ge(&MIN_NODE_LEN) {
              if left.is_some() {
                node.keys[ii] = child.top();
              }
              self.writer.update(ci, child.serialize()?)?;
              return Ok(Some((CursorEntry::Internal(node), successor)));
            };
            if let Some(left) = left {
              let mut left_entry: CursorEntry = self.writer.get(left)?.deserialize()?;
              if left_entry.as_leaf().len().gt(&MIN_NODE_LEN) {
                let (k, p) = left_entry.as_leaf().pop_back().unwrap();
                child.push_front(k.clone(), p);
                node.keys[ii] = k;
                self.writer.update(left, left_entry.serialize()?)?;
                self.writer.update(ci, child.serialize()?)?;
                return Ok(Some((CursorEntry::Internal(node), successor)));
              }
            }

            if let Some(right) = right {
              let mut right_entry: CursorEntry = self.writer.get(right)?.deserialize()?;
              if right_entry.as_leaf().len().gt(&MIN_NODE_LEN) {
                let (k, p) = right_entry.as_leaf().pop_front().unwrap();
                child.push_back(k, p);
                node.keys[ii.add(1)] = right_entry.top();
                self.writer.update(right, right_entry.serialize()?)?;
                self.writer.update(ci, child.serialize()?)?;
                return Ok(Some((CursorEntry::Internal(node), successor)));
              }
            }

            if let Some(left) = left {
              let mut left_entry: CursorEntry = self.writer.get(left)?.deserialize()?;
              left_entry.as_leaf().merge(&mut child);
              node.keys.remove(ii);
              node.children.remove(ii.add(1));
              self.writer.update(left, left_entry.serialize()?)?;
              self.writer.release(ci)?;
              return Ok(Some((CursorEntry::Internal(node), successor)));
            }

            if let Some(right) = right {
              let mut right_entry: CursorEntry = self.writer.get(right)?.deserialize()?;
              child.merge(&mut right_entry.as_leaf());
              node.keys.remove(ii.add(1));
              node.children.remove(ii.add(2));
              self.writer.update(ci, child.serialize()?)?;
              self.writer.release(right)?;
              return Ok(Some((CursorEntry::Internal(node), successor)));
            }
            unreachable!()
          }
        };
      }
      CursorEntry::Leaf(mut node) => {
        let deleted = match node.delete(key) {
          None => return Ok(None),
          Some(i) => i,
        };

        self.writer.release(deleted)?;
        let successor = node.top();
        return Ok(Some((CursorEntry::Leaf(node), successor)));
      }
    }
  }

  pub fn commit(&self) -> Result {
    if self.committed.load(Ordering::SeqCst) {
      return Err(Error::TransactionClosed);
    }

    logger::info(format!("cursor id {} commit start", self.writer.get_id()));
    self.writer.commit()?;
    self.committed.store(true, Ordering::SeqCst);
    Ok(())
  }

  pub fn abort(&self) -> Result {
    println!("abort not implemented");
    Ok(())
  }
}
impl Cursor {
  fn get_index(&self, key: &Vec<u8>) -> Result<usize> {
    let header: TreeHeader = self.writer.get(HEADER_INDEX)?.deserialize()?;
    let mut index = header.get_root();
    loop {
      let entry: CursorEntry = self.writer.get(index)?.deserialize()?;
      match entry.find_or_next(key) {
        Ok(i) => return Ok(i),
        Err(n) => match n {
          Some(i) => index = i,
          None => return Err(Error::NotFound),
        },
      }
    }
  }
}
impl Drop for Cursor {
  fn drop(&mut self) {
    if self.committed.load(Ordering::SeqCst) {
      return;
    }

    logger::warn(format!(
      "cursor id {} which is uncommitted will be abort",
      self.writer.get_id(),
    ));
    self.abort().ok();
  }
}
