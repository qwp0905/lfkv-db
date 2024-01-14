use std::sync::Arc;

use crate::{
  buffer::BufferPool, logger, transaction::LockManager, wal::WAL, Error, Page,
  Result, Serializable,
};

use super::{
  entry::LeafNode, CursorEntry, CursorLocks, CursorWriter, InternalNode,
  TreeHeader, HEADER_INDEX, MAX_NODE_LEN,
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
    logger::info(format!("transaction id {} cursor born", id));
    Self {
      writer: CursorWriter::new(id, wal, buffer),
      locks: CursorLocks::new(locks),
    }
  }

  pub fn initialize(&self) -> Result<()> {
    logger::info(format!("initialize tree header"));
    let header = TreeHeader::initial_state();
    let root = CursorEntry::Leaf(LeafNode::empty());

    self.writer.insert(HEADER_INDEX, header.serialize()?)?;
    self.writer.insert(header.get_root(), root.serialize()?)?;
    Ok(())
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
    logger::info(format!("start to insert {}", &key));
    let page = value.serialize()?;
    match self.get_index(&key) {
      Ok(index) => {
        self.locks.fetch_write(index);
        return self.writer.insert(index, page);
      }
      Err(Error::NotFound) => {
        logger::info(format!("trigger to append {}", &key));
        self.locks.release_all();
        self.locks.fetch_write(HEADER_INDEX);
        let mut header: TreeHeader =
          self.writer.get(HEADER_INDEX)?.deserialize()?;
        let root_index = header.get_root();

        if let Ok((s, i)) =
          self.append_at(&mut header, root_index, key, page)?
        {
          let nri = header.acquire_index();
          let new_root = CursorEntry::Internal(InternalNode {
            keys: vec![s],
            children: vec![root_index, i],
          });
          self.writer.insert(nri, new_root.serialize()?)?;
          header.set_root(nri);
        }

        self.writer.insert(HEADER_INDEX, header.serialize()?)?;
        return Ok(());
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
      let entry: CursorEntry = self.writer.get(index)?.deserialize()?;
      match entry.find_or_next(key) {
        Ok(i) => return Ok(i),
        Err(c) => match c {
          None => return Err(Error::NotFound),
          Some(ci) => {
            index = ci;
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
    let entry: CursorEntry = self.writer.get(index)?.deserialize()?;
    match entry {
      CursorEntry::Internal(mut node) => {
        let i = node.next(&key);
        match self.append_at(header, i, key, page)? {
          Ok((s, ni)) => {
            node.add(s, ni);
            if node.len() <= MAX_NODE_LEN {
              self.writer.insert(index, node.serialize()?)?;
              return Ok(Err(None));
            }

            let (n, m) = node.split();
            let new_i = header.acquire_index();
            self.writer.insert(new_i, n.serialize()?)?;
            self.writer.insert(index, node.serialize()?)?;
            return Ok(Ok((m, new_i)));
          }
          Err(oi) => {
            if let Some(s) = oi {
              node.keys.insert(i - 1, s);
              self.writer.insert(index, node.serialize()?)?;
            };
            return Ok(Err(None));
          }
        };
      }
      CursorEntry::Leaf(mut node) => {
        let pi = header.acquire_index();
        self.writer.insert(pi, page)?;
        let lk = node.add(key, pi);
        if node.len() <= MAX_NODE_LEN {
          self.writer.insert(index, node.serialize()?)?;
          return Ok(Err(lk));
        }

        let ni = header.acquire_index();
        let (n, s) = node.split(index, ni);
        self.writer.insert(ni, n.serialize()?)?;
        self.writer.insert(index, node.serialize()?)?;
        return Ok(Ok((s, ni)));
      }
    }
  }

  // fn find_next(&mut self, key: &String) -> Result<Page> {
  //   self.locks.fetch_read(HEADER_INDEX);
  //   let header: TreeHeader = self.writer.get(HEADER_INDEX)?.deserialize()?;
  //   let mut index = header.get_root();
  //   loop {
  //     self.locks.fetch_read(index);
  //     let entry: CursorEntry = self.writer.get(index)?.deserialize()?;
  //     match entry {
  //       CursorEntry::Internal(node) => {}
  //       CursorEntry::Leaf(node) => {}
  //     }
  //   }
  // }
}

// pub struct CursorIterator<'a> {
//   inner: &'a mut Cursor,
//   base: &'a String,
//   next: Option<usize>,
// }
// impl<'a> Iterator for CursorIterator<'a> {
//   type Item = Page;
//   fn next(&mut self) -> Option<Self::Item> {
//     None
//     // match self.next {
//     // None => {}
//     // Some(i) => {}
//     // }
//   }
// }
