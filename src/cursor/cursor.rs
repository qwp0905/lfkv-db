use std::sync::Arc;

use crate::{
  buffer::BufferPool, transaction::LockManager, wal::WAL, Error, Page, Result,
  Serializable,
};

use super::{
  CursorEntry, CursorLocks, CursorWriter, InternalNode, Node, TreeHeader,
  HEADER_INDEX, MAX_NODE_LEN,
};

pub struct Cursor {
  writer: CursorWriter,
  locks: CursorLocks,
}
impl Cursor {
  pub fn new(
    id: usize,
    buffer: Arc<BufferPool>,
    wal: Arc<WAL>,
    locks: Arc<LockManager>,
  ) -> Self {
    Self {
      writer: CursorWriter::new(id, wal, buffer),
      locks: CursorLocks::new(locks),
    }
  }

  pub fn get<T>(&mut self, key: String) -> Result<T>
  where
    T: Serializable,
  {
    let index = self.get_index(&key)?;
    self.locks.fetch_read(index);
    self.writer.get(index).and_then(|page| page.deserialize())
  }

  pub fn insert<T>(&mut self, key: String, value: T) -> Result<()>
  where
    T: Serializable,
  {
    let page = value.serialize()?;
    match self.get_index(&key) {
      Ok(index) => {
        self.locks.fetch_write(index);
        return self.writer.insert(index, page);
      }
      Err(Error::NotFound) => {
        self.locks.release_all();
        self.locks.fetch_write(HEADER_INDEX);
        let mut header: TreeHeader =
          self.writer.get(HEADER_INDEX)?.deserialize()?;
        let root_index = header.get_root();

        match self.append_at(&mut header, root_index, key, page)? {
          Ok((s, i)) => {
            let nri = header.acquire_index();
            let new_root = Node::Internal(InternalNode {
              keys: vec![s],
              children: vec![root_index, i],
            });
            self.writer.insert(nri, new_root.serialize()?)?;
            header.set_root(nri);
            self.writer.insert(HEADER_INDEX, header.serialize()?)?;
            return Ok(());
          }
          Err(_) => return Ok(()),
        }
      }
      Err(err) => return Err(err),
    }
  }
}

impl Cursor {
  fn get_index(&mut self, key: &String) -> Result<usize> {
    self.locks.fetch_read(HEADER_INDEX);
    let header: TreeHeader = self.writer.get(HEADER_INDEX)?.deserialize()?;
    let mut index = header.get_root();
    loop {
      self.locks.fetch_read(index);
      let page = self.writer.get(index)?;
      let entry = CursorEntry::from(index, page)?;
      match entry.find_next(key) {
        Ok(i) => return Ok(i),
        Err(c) => match c {
          None => return Err(Error::NotFound),
          Some(i) => {
            index = i;
          }
        },
      }
    }
  }

  fn append_at(
    &mut self,
    header: &mut TreeHeader,
    index: usize,
    key: String,
    page: Page,
  ) -> Result<core::result::Result<(String, usize), Option<String>>> {
    self.locks.fetch_write(index);
    let entry = CursorEntry::from(index, self.writer.get(index)?)?;
    match entry.node {
      Node::Internal(mut node) => {
        let i = node.next(&key);
        match self.append_at(header, index, key, page)? {
          Ok((s, ni)) => {
            node.add(s, ni);
            if node.len() <= MAX_NODE_LEN {
              let np = node.serialize()?;
              self.writer.insert(index, np)?;
              return Ok(Err(None));
            }

            let (n, s) = node.split();
            let ni = header.acquire_index();
            self.writer.insert(ni, n.serialize()?)?;
            self.writer.insert(index, node.serialize()?)?;
            return Ok(Ok((s, ni)));
          }
          Err(oi) => {
            if let Some(s) = oi {
              node.keys.insert(i, s);
              self.writer.insert(index, node.serialize()?)?;
            };
            return Ok(Err(None));
          }
        };
      }
      Node::Leaf(mut node) => {
        let i = header.acquire_index();
        self.writer.insert(i, page)?;
        let lk = node.add(key, i);
        if node.len() <= MAX_NODE_LEN {
          let np = node.serialize()?;
          self.writer.insert(index, np)?;
          return Ok(Err(lk));
        }

        let (n, s) = node.split(index, i);
        let ni = header.acquire_index();
        self.writer.insert(ni, n.serialize()?)?;
        self.writer.insert(index, node.serialize()?)?;
        return Ok(Ok((s, ni)));
      }
    }
  }
}
