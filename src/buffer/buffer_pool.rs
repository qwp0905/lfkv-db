use std::{
  collections::{BTreeMap, VecDeque},
  sync::{Arc, Mutex, RwLock},
};

use crate::{
  disk::PageSeeker, transaction::LockManager, Page, Result, StoppableChannel,
  ThreadPool,
};

use super::Cache;

pub struct BufferPool {
  cache: Arc<Mutex<Cache<(), ()>>>,
  disk: Arc<PageSeeker>,
  locks: Arc<LockManager>,
  dirty: RwLock<BTreeMap<usize, Page>>,
  background: ThreadPool<Result<()>>,
  readonly: Arc<VecDeque<BTreeMap<usize, Page>>>,
  flush_c: StoppableChannel<()>,
}
impl BufferPool {
  pub fn get(&self, index: usize) {}
}
