use std::{
  collections::{HashSet, VecDeque},
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::Duration,
};

use super::{
  CursorNode, DataEntry, Pointer, RecordData, TreeHeader, VersionRecord, HEADER_INDEX,
};
use crate::{
  buffer_pool::BufferPool,
  error::Result,
  serialize::SerializeFrom,
  thread::{SharedWorkThread, SingleWorkThread, WorkBuilder},
  transaction::{FreeList, VersionVisibility},
  utils::ToArc,
  wal::WAL,
};

pub struct GarbageCollectionConfig {
  pub interval: Duration,
  pub thread_count: usize,
}

pub struct GarbageCollector {
  clean_entry: SingleWorkThread<(), Result>,
  clean_leaf: SingleWorkThread<(), Result>,
  clean_version_chain: Arc<SharedWorkThread<Pointer, Result>>,
  check_empty: Arc<SharedWorkThread<Pointer, Result<bool>>>,
  release: Arc<SharedWorkThread<Pointer, Result>>,
  buffer_pool: Arc<BufferPool>,
  free_list: Arc<FreeList>,
  initialized: Arc<AtomicBool>,
}
impl GarbageCollector {
  pub fn clean_entry(&self) -> Result {
    self.clean_entry.send_await(())?
  }
  pub fn clean_leaf(&self) -> Result {
    self.clean_leaf.send_await(())?
  }
  pub fn new(
    buffer_pool: Arc<BufferPool>,
    version_visibility: Arc<VersionVisibility>,
    free_list: Arc<FreeList>,
    wal: Arc<WAL>,
    config: GarbageCollectionConfig,
  ) -> Self {
    let initialized = Arc::new(AtomicBool::new(false));

    let check_empty = WorkBuilder::new()
      .name("gc check version chain emtpy")
      .stack_size(2 << 20)
      .shared(config.thread_count)
      .build_unchecked(run_check_empty(buffer_pool.clone()))
      .to_arc();

    let release = WorkBuilder::new()
      .name("gc release orphand pages")
      .stack_size(2 << 20)
      .shared(config.thread_count)
      .build_unchecked(run_release(free_list.clone()))
      .to_arc();

    let clean_version_chain = WorkBuilder::new()
      .name("gc clean version chains")
      .stack_size(2 << 20)
      .shared(config.thread_count)
      .build_unchecked(run_clean_version_chain(
        buffer_pool.clone(),
        version_visibility.clone(),
        wal.clone(),
        release.clone(),
      ))
      .to_arc();

    let clean_entry = WorkBuilder::new()
      .name("gc clean entries")
      .stack_size(2 << 20)
      .single()
      .with_timeout(
        config.interval,
        run_clean_entry(
          buffer_pool.clone(),
          version_visibility.clone(),
          clean_version_chain.clone(),
          initialized.clone(),
        ),
      );

    let clean_leaf = WorkBuilder::new()
      .name("gc clean leaf nodes")
      .stack_size(2 << 20)
      .single()
      .with_timeout(
        config.interval,
        run_clean_leaf(
          buffer_pool.clone(),
          wal.clone(),
          check_empty.clone(),
          release.clone(),
          initialized.clone(),
        ),
      );

    Self {
      clean_entry,
      clean_leaf,
      clean_version_chain,
      check_empty,
      release,
      buffer_pool,
      free_list,
      initialized,
    }
  }

  pub fn release_orphand(&self, end: usize) -> Result {
    let (mut free_indexes, free_visited) = self.free_list.get_all()?;
    let mut visited: HashSet<usize> =
      vec![HEADER_INDEX].into_iter().chain(free_visited).collect();

    let root = self
      .buffer_pool
      .read(HEADER_INDEX)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();
    let mut node_stack = vec![root];
    let mut entry_stack = vec![];

    while let Some(index) = node_stack.pop() {
      visited.insert(index);
      match self
        .buffer_pool
        .read(index)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
      {
        CursorNode::Internal(internal) => node_stack.extend(internal.get_all_child()),
        CursorNode::Leaf(leaf) => entry_stack.extend(leaf.get_entry_pointers()),
      };
    }

    while let Some(index) = entry_stack.pop() {
      visited.insert(index);
      let entry: DataEntry = self
        .buffer_pool
        .read(index)?
        .for_read()
        .as_ref()
        .deserialize()?;
      entry.get_next().map(|i| entry_stack.push(i));
    }

    for index in 0..end {
      if visited.remove(&index) || free_indexes.remove(&index) {
        continue;
      }
      self.release.send_no_wait(index);
    }

    self.initialized.store(true, Ordering::Release);
    Ok(())
  }

  pub fn close(&self) {
    self.clean_entry.close();
    self.clean_version_chain.close();
    self.clean_leaf.close();
    self.check_empty.close();
    self.release.close();
  }
}

fn run_clean_entry(
  buffer_pool: Arc<BufferPool>,
  version_visibility: Arc<VersionVisibility>,
  clean_version_chain: Arc<SharedWorkThread<Pointer, Result>>,
  initialized: Arc<AtomicBool>,
) -> impl Fn(Option<()>) -> Result {
  move |_| {
    if !initialized.load(Ordering::Acquire) {
      return Ok(());
    }

    let mut index = buffer_pool
      .read(HEADER_INDEX)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    while let CursorNode::Internal(node) = buffer_pool
      .read(index)?
      .for_read()
      .as_ref()
      .deserialize::<CursorNode>()?
    {
      index = node.first_child()
    }

    let min_version = version_visibility.min_version();

    let mut index = Some(index);
    while let Some(i) = index.take() {
      let leaf = buffer_pool
        .read(i)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
        .as_leaf()?;

      clean_version_chain
        .send_batch(leaf.get_entry_pointers())
        .wait()?
        .into_iter()
        .fold(Ok(()), |a, c| a.and_then(|_| c))?;

      index = leaf.get_next();
    }

    version_visibility.remove_aborted(&min_version);
    Ok(())
  }
}

fn run_clean_leaf(
  buffer_pool: Arc<BufferPool>,
  wal: Arc<WAL>,
  check_empty: Arc<SharedWorkThread<Pointer, Result<bool>>>,
  release: Arc<SharedWorkThread<Pointer, Result>>,
  initialized: Arc<AtomicBool>,
) -> impl Fn(Option<()>) -> Result {
  move |_| {
    if !initialized.load(Ordering::Acquire) {
      return Ok(());
    }

    let mut index = buffer_pool
      .read(HEADER_INDEX)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    while let CursorNode::Internal(node) = buffer_pool
      .read(index)?
      .for_read()
      .as_ref()
      .deserialize::<CursorNode>()?
    {
      index = node.first_child()
    }

    let mut index = Some(index);
    while let Some(i) = index.take() {
      let mut slot = buffer_pool.read(i)?.for_write();
      let mut leaf = slot.as_ref().deserialize::<CursorNode>()?.as_leaf()?;
      index = leaf.get_next();

      let prev_len = leaf.len();
      let mut waiting = vec![];

      for (key, ptr) in leaf.drain() {
        waiting.push((key, ptr, check_empty.send(ptr)));
      }

      let mut new_entries = vec![];

      for (key, ptr, is_empty) in waiting {
        if is_empty.wait_flatten()? {
          release.send_no_wait(ptr);
          continue;
        }
        new_entries.push((key, ptr))
      }

      if new_entries.len() == prev_len {
        continue;
      }

      leaf.set_entries(new_entries);
      slot.as_mut().serialize_from(&CursorNode::Leaf(leaf))?;
      wal.append_insert(0, slot.get_index(), slot.as_ref())?;
    }

    Ok(())
  }
}

fn run_check_empty(buffer_pool: Arc<BufferPool>) -> impl Fn(Pointer) -> Result<bool> {
  move |ptr| {
    let entry: DataEntry = buffer_pool.read(ptr)?.for_read().as_ref().deserialize()?;
    Ok(entry.is_empty())
  }
}

fn run_release(free: Arc<FreeList>) -> impl Fn(Pointer) -> Result {
  move |ptr| free.dealloc(ptr)
}

fn run_clean_version_chain(
  buffer_pool: Arc<BufferPool>,
  version_visibility: Arc<VersionVisibility>,
  wal: Arc<WAL>,
  release: Arc<SharedWorkThread<Pointer, Result>>,
) -> impl Fn(Pointer) -> Result {
  move |ptr| {
    let mut index = Some(ptr);
    let mut max_found = false;

    while let Some(i) = index.take() {
      let mut slot = buffer_pool.read(i)?.for_write();
      let mut entry: DataEntry = slot.as_ref().deserialize()?;

      let prev_len = entry.len();
      let mut expired_max: Option<VersionRecord> = None;
      let min_version = version_visibility.min_version();
      let mut new_versions = VecDeque::new();
      for record in entry.drain() {
        if version_visibility.is_aborted(&record.owner) {
          continue;
        }
        if record.version > min_version {
          new_versions.push_back(record);
          continue;
        }
        if max_found {
          continue;
        }

        match expired_max.as_mut() {
          Some(max) if max.version < record.version => *max = record,
          None => expired_max = Some(record),
          _ => {}
        }
      }

      if !max_found {
        if let Some(record) = expired_max.take() {
          if let RecordData::Data(_) = &record.data {
            new_versions.push_back(record);
          }

          max_found = true;
        }
      }

      if new_versions.len() == prev_len {
        index = entry.get_next();
        continue;
      }

      if new_versions.len() > 0 {
        entry.set_records(new_versions);
        slot.as_mut().serialize_from(&entry)?;
        wal.append_insert(0, slot.get_index(), slot.as_ref())?;
        index = entry.get_next();
        continue;
      }

      let next = match entry.get_next() {
        Some(next) => next,
        None => {
          slot.as_mut().serialize_from(&entry)?;
          return wal.append_insert(0, slot.get_index(), slot.as_ref());
        }
      };

      let next_entry: DataEntry =
        buffer_pool.read(next)?.for_read().as_ref().deserialize()?;
      slot.as_mut().serialize_from(&next_entry)?;
      wal.append_insert(0, slot.get_index(), slot.as_ref())?;
      index = Some(i);

      release.send_no_wait(next);
    }
    Ok(())
  }
}
