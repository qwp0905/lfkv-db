use std::sync::{Arc, RwLock};

use crate::{
  buffer::{BufferPool, BLOCK_SIZE},
  disk::FreeList,
  logger,
  wal::WriteAheadLog,
  Error, Result, Serializable, ShortenedRwLock,
};

use super::{
  CursorEntry, CursorWriter, InternalNode, LeafNode, TreeHeader, HEADER_INDEX,
  MAX_NODE_LEN,
};

pub struct Cursor {
  committed: Arc<RwLock<bool>>,
  freelist: Arc<FreeList<BLOCK_SIZE>>,
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
      committed: Arc::new(RwLock::new(false)),
      freelist,
      writer: CursorWriter::new(tx_id, last_commit_index, wal, buffer),
    })
  }

  pub fn initialize(&self) -> Result {
    if let Err(Error::NotFound) = self.writer.get::<TreeHeader>(HEADER_INDEX) {
      logger::info(format!("there are no tree header and will be initialized"));
      let header = TreeHeader::initial_state();
      let root = header.get_root();
      self.writer.insert(HEADER_INDEX, header)?;
      self
        .writer
        .insert(root, CursorEntry::Leaf(LeafNode::empty()))?;
    };
    Ok(())
  }

  pub fn get<T>(&self, key: &Vec<u8>) -> Result<T>
  where
    T: Serializable,
  {
    if self.committed.rl().eq(&true) {
      return Err(Error::TransactionClosed);
    }

    let i = self.get_index(key)?;
    self.writer.get(i)
  }

  pub fn insert<T>(&self, key: Vec<u8>, value: T) -> Result
  where
    T: Serializable,
  {
    if self.committed.rl().eq(&true) {
      return Err(Error::TransactionClosed);
    }

    match self.get_index(&key) {
      Ok(index) => self.writer.insert(index, value),
      Err(Error::NotFound) => {
        let mut header: TreeHeader = self.writer.get(HEADER_INDEX)?;
        if let Ok((s, i)) = self.append_at(header.get_root(), key, value)? {
          let nri = self.freelist.acquire();
          let new_root = CursorEntry::Internal(InternalNode {
            keys: vec![s],
            children: vec![header.get_root(), i],
          });
          self.writer.insert(nri, new_root)?;

          header.set_root(nri);
          self.writer.insert(HEADER_INDEX, header)?;
        }
        Ok(())
      }
      Err(err) => Err(err),
    }
  }

  pub fn commit(&self) -> Result {
    let mut committed = self.committed.wl();
    if committed.eq(&true) {
      return Err(Error::TransactionClosed);
    }

    logger::info(format!("cursor id {} commit start", self.writer.get_id()));
    self.writer.commit()?;
    *committed = true;
    Ok(())
  }

  pub fn abort(&self) -> Result {
    println!("abort not implemented");
    Ok(())
  }
}
impl Cursor {
  fn get_index(&self, key: &Vec<u8>) -> Result<usize> {
    let header: TreeHeader = self.writer.get(HEADER_INDEX)?;
    let mut index = header.get_root();
    loop {
      let entry: CursorEntry = self.writer.get(index)?;
      match entry.find_or_next(key) {
        Ok(i) => return Ok(i),
        Err(n) => match n {
          Some(i) => index = i,
          None => return Err(Error::NotFound),
        },
      }
    }
  }

  fn append_at<T>(
    &self,
    current: usize,
    key: Vec<u8>,
    value: T,
  ) -> Result<core::result::Result<(Vec<u8>, usize), Option<Vec<u8>>>>
  where
    T: Serializable,
  {
    let entry: CursorEntry = self.writer.get(current)?;
    match entry {
      CursorEntry::Internal(mut node) => {
        let i = node.next(&key);
        match self.append_at(i, key, value)? {
          Ok((s, ni)) => {
            node.add(s, ni);
            if node.len().le(&MAX_NODE_LEN) {
              self.writer.insert(current, node)?;
              return Ok(Err(None));
            }

            let (n, s) = node.split();
            let new_i = self.freelist.acquire();
            self.writer.insert(new_i, n)?;
            self.writer.insert(current, node)?;
            Ok(Ok((s, new_i)))
          }
          Err(oi) => {
            if let Some(s) = oi {
              node.keys.insert(i - 1, s);
              self.writer.insert(current, node)?;
            }
            Ok(Err(None))
          }
        }
      }
      CursorEntry::Leaf(mut node) => {
        if let Some(i) = node.find(&key) {
          self.writer.insert(node.keys[i].1, value)?;
          return Ok(Err(None));
        };

        let pi = self.freelist.acquire();
        self.writer.insert(pi, value)?;
        let lk = node.add(key, pi);
        if node.len() <= MAX_NODE_LEN {
          self.writer.insert(current, node)?;
          return Ok(Err(lk));
        }

        let ni = self.freelist.acquire();
        let (n, s) = node.split(current, ni);
        self.writer.insert(ni, n)?;
        self.writer.insert(current, node)?;
        Ok(Ok((s, ni)))
      }
    }
  }
}
impl Drop for Cursor {
  fn drop(&mut self) {
    if self.committed.rl().eq(&false) {
      logger::warn(format!(
        "cursor id {} which is uncommitted will be abort",
        self.writer.get_id(),
      ));
      self.abort().ok();
    }
  }
}

// pub struct Cursor {
//   writer: CursorWriter,
//   locks: CursorLocks,
// }
// impl Cursor {
//   pub fn new(
//     id: usize,
//     buffer: Arc<BufferPool>,
//     wal: Arc<WAL>,
//     locks: Arc<LockManager>,
//   ) -> Self {
//     logger::info(format!("transaction id {} cursor born", id));
//     Self {
//       writer: CursorWriter::new(id, wal, buffer),
//       locks: CursorLocks::new(locks),
//     }
//   }

//   pub fn initialize(&self) -> Result<()> {
//     logger::info(format!("initialize tree header"));
//     let header = TreeHeader::initial_state();
//     let root = CursorEntry::Leaf(LeafNode::empty());

//     self.writer.insert(HEADER_INDEX, header.serialize()?)?;
//     self.writer.insert(header.get_root(), root.serialize()?)?;
//     Ok(())
//   }

//   pub fn get<T>(&mut self, key: String) -> Result<T>
//   where
//     T: Serializable,
//   {
//     let index = self.get_index(&key)?;
//     self.locks.fetch_read(index);
//     self.writer.get(index).and_then(|page| page.deserialize())
//   }

//   pub fn insert<T>(&mut self, key: String, value: T) -> Result<()>
//   where
//     T: Serializable,
//   {
//     logger::info(format!("start to insert {}", &key));
//     let page = value.serialize()?;
//     match self.get_index(&key) {
//       Ok(index) => {
//         self.locks.fetch_write(index);
//         return self.writer.insert(index, page);
//       }
//       Err(Error::NotFound) => {
//         self.locks.release_all();
//         self.locks.fetch_write(HEADER_INDEX);
//         logger::info(format!("start to append {key}"));
//         let mut header: TreeHeader =
//           self.writer.get(HEADER_INDEX)?.deserialize()?;
//         let root_index = header.get_root();

//         if let Ok((s, i)) =
//           self.append_at(&mut header, root_index, key, page)?
//         {
//           let nri = header.acquire_index();
//           let new_root = CursorEntry::Internal(InternalNode {
//             keys: vec![s],
//             children: vec![root_index, i],
//           });
//           self.writer.insert(nri, new_root.serialize()?)?;
//           header.set_root(nri);
//         }

//         self.writer.insert(HEADER_INDEX, header.serialize()?)?;
//         return Ok(());
//       }
//       Err(err) => return Err(err),
//     }
//   }

//   pub fn scan<T>(
//     &mut self,
//     prefix: String,
//   ) -> Result<impl Iterator<Item = T> + '_>
//   where
//     T: Serializable,
//   {
//     self.locks.fetch_read(HEADER_INDEX);
//     let header: TreeHeader = self.writer.get(HEADER_INDEX)?.deserialize()?;
//     let index = header.get_root();
//     let (node, i) = self.first_entry_at(index, &prefix)?;

//     let iter = CursorIterator {
//       inner: self,
//       current_node: node,
//       current_index: i,
//       prefix,
//     };

//     return Ok(iter.map_while(|page| page.deserialize().ok()));
//   }
// }

// impl Cursor {
//   fn get_index(&mut self, key: &String) -> Result<usize> {
//     self.locks.fetch_read(HEADER_INDEX);
//     let header: TreeHeader = self.writer.get(HEADER_INDEX)?.deserialize()?;
//     let mut index = header.get_root();
//     loop {
//       self.locks.fetch_read(index);
//       let entry: CursorEntry = self.writer.get(index)?.deserialize()?;
//       match entry.find_or_next(key) {
//         Ok(i) => return Ok(i),
//         Err(c) => match c {
//           None => return Err(Error::NotFound),
//           Some(ci) => {
//             index = ci;
//           }
//         },
//       }
//     }
//   }

//   fn append_at(
//     &mut self,
//     header: &mut TreeHeader,
//     index: usize,
//     key: String,
//     page: Page,
//   ) -> Result<core::result::Result<(String, usize), Option<String>>> {
//     self.locks.fetch_write(index);
//     let entry: CursorEntry = self.writer.get(index)?.deserialize()?;
//     match entry {
//       CursorEntry::Internal(mut node) => {
//         let i = node.next(&key);
//         match self.append_at(header, i, key, page)? {
//           Ok((s, ni)) => {
//             node.add(s, ni);
//             if node.len() <= MAX_NODE_LEN {
//               self.writer.insert(index, node.serialize()?)?;
//               return Ok(Err(None));
//             }

//             let (n, m) = node.split();
//             let new_i = header.acquire_index();
//             self.writer.insert(new_i, n.serialize()?)?;
//             self.writer.insert(index, node.serialize()?)?;
//             return Ok(Ok((m, new_i)));
//           }
//           Err(oi) => {
//             // 이부분 수정 필요할듯... 만약 이전엔 없었는데 새로 추가되었다면 업데이트하도록 수정 필요... 아니면 인서트와 업데이트를 명시적으로 분리 필요,,
//             if let Some(s) = oi {
//               node.keys.insert(i - 1, s);
//               self.writer.insert(index, node.serialize()?)?;
//             };
//             return Ok(Err(None));
//           }
//         };
//       }
//       CursorEntry::Leaf(mut node) => {
//         if let Some(i) = node.find(&key) {
//           self.writer.insert(node.keys[i].1, page)?;
//           return Ok(Err(None));
//         };

//         let pi = header.acquire_index();
//         self.writer.insert(pi, page)?;
//         let lk = node.add(key, pi);
//         if node.len() <= MAX_NODE_LEN {
//           self.writer.insert(index, node.serialize()?)?;
//           return Ok(Err(lk));
//         }

//         let ni = header.acquire_index();
//         let (n, s) = node.split(index, ni);
//         self.writer.insert(ni, n.serialize()?)?;
//         self.writer.insert(index, node.serialize()?)?;
//         return Ok(Ok((s, ni)));
//       }
//     }
//   }

//   fn first_entry_at(
//     &mut self,
//     index: usize,
//     last_key: &String,
//   ) -> Result<(LeafNode, usize)> {
//     self.locks.fetch_read(index);
//     let entry: CursorEntry = self.writer.get(index)?.deserialize()?;
//     match entry {
//       CursorEntry::Internal(node) => {
//         return self.first_entry_at(node.next(last_key), last_key)
//       }
//       CursorEntry::Leaf(node) => {
//         match node.keys.binary_search_by(|(k, _)| k.cmp(last_key)) {
//           Ok(i) => return Ok((node, i)),
//           Err(i) => {
//             if i <= node.len() {
//               return Ok((node, i));
//             };
//             if let Some(next) = node.next {
//               self.locks.fetch_read(next);
//               let ne: CursorEntry = self.writer.get(next)?.deserialize()?;
//               if let CursorEntry::Leaf(ne) = ne {
//                 return Ok((ne, 0));
//               }
//             };
//             return Err(Error::NotFound);
//           }
//         }
//       }
//     }
//   }
// }

// pub struct CursorIterator<'a> {
//   inner: &'a mut Cursor,
//   current_node: LeafNode,
//   current_index: usize,
//   prefix: String,
// }
// impl<'a> Iterator for CursorIterator<'a> {
//   type Item = Page;
//   fn next(&mut self) -> Option<Self::Item> {
//     if self.current_index >= self.current_node.len() {
//       let next = match self.current_node.next {
//         None => return None,
//         Some(next) => next,
//       };

//       self.inner.locks.fetch_read(next);
//       self.current_node = match self
//         .inner
//         .writer
//         .get(next)
//         .and_then(|page| page.deserialize())
//       {
//         Ok(e) => match e {
//           CursorEntry::Leaf(node) => node,
//           _ => return None,
//         },
//         _ => return None,
//       };

//       self.current_index = 0;
//     }

//     let (key, index) = &self.current_node.keys[self.current_index];
//     if !key.starts_with(&self.prefix) {
//       return None;
//     }
//     self.current_index += 1;
//     self.inner.locks.fetch_read(*index);
//     match self.inner.writer.get(*index) {
//       Ok(page) => return Some(page),
//       _ => return None,
//     };
//   }
// }
