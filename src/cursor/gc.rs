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
  thread::{SharedWorkThread, SingleWorkThread, WorkBuilder},
  transaction::{FreeList, PageRecorder, VersionVisibility},
  utils::ToArc,
};

pub struct GarbageCollectionConfig {
  pub interval: Duration,
  pub thread_count: usize,
}

pub struct GarbageCollector {
  main: SingleWorkThread<(), Result>,
  check: Arc<SharedWorkThread<Pointer, Result<bool>>>,
  entry: Arc<SharedWorkThread<Pointer, Result>>,
  release: Arc<SharedWorkThread<Pointer, Result>>,
  buffer_pool: Arc<BufferPool>,
  free_list: Arc<FreeList>,
  initialized: Arc<AtomicBool>,
}
impl GarbageCollector {
  pub fn run(&self) -> Result {
    self.main.send_await(())?
  }

  pub fn start(
    buffer_pool: Arc<BufferPool>,
    version_visibility: Arc<VersionVisibility>,
    free_list: Arc<FreeList>,
    recorder: Arc<PageRecorder>,
    config: GarbageCollectionConfig,
  ) -> Self {
    let initialized = AtomicBool::new(false).to_arc();
    let release = WorkBuilder::new()
      .name("gc release entry")
      .stack_size(2 << 20)
      .shared(config.thread_count)
      .build_unchecked(run_release(free_list.clone()))
      .to_arc();
    let entry = WorkBuilder::new()
      .name("gc found entry")
      .stack_size(2 << 20)
      .shared(config.thread_count)
      .build_unchecked(run_entry(
        buffer_pool.clone(),
        version_visibility.clone(),
        recorder.clone(),
        release.clone(),
      ))
      .to_arc();
    let check = WorkBuilder::new()
      .name("gc check top entry")
      .stack_size(2 << 20)
      .shared(config.thread_count)
      .build_unchecked(run_check(buffer_pool.clone()))
      .to_arc();
    let main = WorkBuilder::new()
      .name("gc main")
      .stack_size(2 << 20)
      .single()
      .with_timeout(
        config.interval,
        run_main(
          buffer_pool.clone(),
          version_visibility.clone(),
          recorder.clone(),
          entry.clone(),
          check.clone(),
          release.clone(),
          initialized.clone(),
        ),
      );
    Self {
      main,
      check,
      entry,
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
    self.main.close();
    self.check.close();
    self.entry.close();
    self.release.close();
  }
}

fn run_main(
  buffer_pool: Arc<BufferPool>,
  version_visibility: Arc<VersionVisibility>,
  recorder: Arc<PageRecorder>,
  entry_c: Arc<SharedWorkThread<Pointer, Result>>,
  check_c: Arc<SharedWorkThread<Pointer, Result<bool>>>,
  release_c: Arc<SharedWorkThread<Pointer, Result>>,
  initialized: Arc<AtomicBool>,
) -> impl Fn(Option<()>) -> Result {
  move |_| {
    if !initialized.load(Ordering::Acquire) {
      return Ok(());
    }
    let header: TreeHeader = buffer_pool
      .read(HEADER_INDEX)?
      .for_read()
      .as_ref()
      .deserialize()?;

    let mut index = header.get_root();
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
      {
        let leaf = buffer_pool
          .read(i)?
          .for_read()
          .as_ref()
          .deserialize::<CursorNode>()?
          .as_leaf()?;
        entry_c
          .send_batch(leaf.get_entry_pointers())
          .wait()?
          .into_iter()
          .collect::<Result>()?;
      }

      let mut slot = buffer_pool.read(i)?.for_write();
      let mut leaf = slot.as_ref().deserialize::<CursorNode>()?.as_leaf()?;
      index = leaf.get_next();

      let prev_len = leaf.len();
      let mut new_entries = vec![];
      let mut orphand = vec![];

      let found = leaf
        .drain()
        .map(|(key, ptr)| (key, ptr, check_c.send(ptr)))
        .collect::<Vec<_>>();
      for (key, ptr, r) in found.into_iter() {
        if r.wait_flatten()? {
          orphand.push(ptr);
        } else {
          new_entries.push((key, ptr));
        }
      }

      if new_entries.len() == prev_len {
        continue;
      }

      leaf.set_entries(new_entries);
      recorder.serialize_and_log(0, &mut slot, &CursorNode::Leaf(leaf))?;
      drop(slot);

      orphand.into_iter().for_each(|p| release_c.send_no_wait(p))
    }

    version_visibility.remove_aborted(&min_version);
    Ok(())
  }
}

fn run_release(free_list: Arc<FreeList>) -> impl Fn(Pointer) -> Result {
  let free_list = free_list.clone();
  move |pointer: Pointer| free_list.dealloc(pointer)
}

fn run_entry(
  buffer_pool: Arc<BufferPool>,
  version_visibility: Arc<VersionVisibility>,
  recorder: Arc<PageRecorder>,
  release_c: Arc<SharedWorkThread<Pointer, Result>>,
) -> impl Fn(Pointer) -> Result {
  let buffer_pool = buffer_pool.clone();
  let release_c = release_c.clone();
  let version_visibility = version_visibility.clone();
  move |ptr: Pointer| {
    let mut index = Some(ptr);
    let mut max_found = false;

    while let Some(i) = index.take() {
      let mut slot = buffer_pool.read(i)?.for_write();
      let mut entry: DataEntry = slot.as_ref().deserialize()?;

      let prev_len = entry.len();
      let mut expired_max: Option<VersionRecord> = None;
      let min_version = version_visibility.min_version();
      let mut new_versions = VecDeque::new();
      for record in entry.take_versions() {
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
        entry.set_versions(new_versions);
        recorder.serialize_and_log(0, &mut slot, &entry)?;
        index = entry.get_next();
        continue;
      }

      let next = match entry.get_next() {
        Some(next) => next,
        None => return recorder.serialize_and_log(0, &mut slot, &entry),
      };

      let next_entry: DataEntry =
        buffer_pool.read(next)?.for_read().as_ref().deserialize()?;
      recorder.serialize_and_log(0, &mut slot, &next_entry)?;
      index = Some(i);

      release_c.send_no_wait(next);
    }
    Ok(())
  }
}

fn run_check(buffer_pool: Arc<BufferPool>) -> impl Fn(Pointer) -> Result<bool> {
  let buffer_pool = buffer_pool.clone();
  move |pointer: Pointer| {
    Ok(
      buffer_pool
        .read(pointer)?
        .for_read()
        .as_ref()
        .deserialize::<DataEntry>()?
        .is_empty(),
    )
  }
}
