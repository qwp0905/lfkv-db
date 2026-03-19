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
  thread::{BackgroundThread, WorkBuilder},
  transaction::{FreeList, PageRecorder, VersionVisibility},
  utils::{DoubleBuffer, LogFilter, ToArc, ToBox},
};

pub struct GarbageCollectionConfig {
  pub interval: Duration,
  pub thread_count: usize,
}

enum GcPointer {
  Trim(Pointer),    // release without head of data entry.
  Release(Pointer), // release head of data entry lazyly.
}

pub struct GarbageCollector {
  clean_leaf: Box<dyn BackgroundThread<(), Result>>,
  check: Arc<dyn BackgroundThread<Pointer, Result<bool>>>,
  entry: Arc<dyn BackgroundThread<Pointer, Result>>,
  release: Arc<dyn BackgroundThread<Pointer, Result>>,
  buffer_pool: Arc<BufferPool>,
  free_list: Arc<FreeList>,
  initialized: Arc<AtomicBool>,
  queue: Arc<DoubleBuffer<GcPointer>>,
  logger: LogFilter,
}
impl GarbageCollector {
  pub fn run(&self) -> Result {
    let queue = self.queue.switch();
    self.logger.debug(format!(
      "{} data will check version in this scope.",
      queue.len()
    ));
    let mut waiting = Vec::new();
    let mut release = Vec::new();
    let mut dedup = HashSet::new();
    while let Some(ptr) = queue.pop() {
      match ptr {
        GcPointer::Trim(ptr) => {
          if !dedup.insert(ptr) {
            continue;
          }
          waiting.push(self.entry.send(ptr));
        }
        GcPointer::Release(ptr) => release.push(ptr),
      }
    }
    self.logger.debug("all entry cleaning triggered.");

    waiting
      .into_iter()
      .map(|v| v.wait_flatten())
      .collect::<Result>()?;
    self.logger.debug("unreachable versions all collected.");
    self.release.send_batch(release);
    Ok(())
  }

  pub fn mark(&self, pointer: Pointer) {
    self.queue.push(GcPointer::Trim(pointer))
  }

  pub fn start(
    buffer_pool: Arc<BufferPool>,
    version_visibility: Arc<VersionVisibility>,
    free_list: Arc<FreeList>,
    recorder: Arc<PageRecorder>,
    logger: LogFilter,
    config: GarbageCollectionConfig,
  ) -> Self {
    let initialized = AtomicBool::new(false).to_arc();
    let queue = DoubleBuffer::new().to_arc();
    let release = WorkBuilder::new()
      .name("gc release entry")
      .stack_size(2 << 20)
      .shared(config.thread_count)
      .build(run_release(free_list.clone()))
      .to_arc();
    let entry = WorkBuilder::new()
      .name("gc found entry")
      .stack_size(2 << 20)
      .shared(config.thread_count)
      .build(run_entry(
        buffer_pool.clone(),
        version_visibility,
        recorder.clone(),
        release.clone(),
      ))
      .to_arc();
    let check = WorkBuilder::new()
      .name("gc check top entry")
      .stack_size(2 << 20)
      .shared(config.thread_count)
      .build(run_check(buffer_pool.clone()))
      .to_arc();
    let clean_leaf = WorkBuilder::new()
      .name("gc clean leaf")
      .stack_size(2 << 20)
      .single()
      .interval(
        config.interval,
        run_clean_leaf(
          buffer_pool.clone(),
          recorder.clone(),
          check.clone(),
          queue.clone(),
          initialized.clone(),
          logger.clone(),
        ),
      )
      .to_box();

    Self {
      clean_leaf,
      check,
      entry,
      release,
      buffer_pool,
      free_list,
      initialized,
      queue,
      logger,
    }
  }
  pub fn ready(&self) {
    self.initialized.store(true, Ordering::Release);
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

    // push to queue for initial checkpoint
    for &pointer in entry_stack.iter() {
      self.queue.push(GcPointer::Trim(pointer));
    }
    self.logger.debug(format!(
      "{} entry queued for initial gc.",
      entry_stack.len()
    ));

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

    Ok(())
  }

  pub fn close(&self) {
    self.clean_leaf.close();
    self.check.close();
    self.entry.close();
    self.release.close();
  }
}

fn run_clean_leaf(
  buffer_pool: Arc<BufferPool>,
  recorder: Arc<PageRecorder>,
  check_c: Arc<dyn BackgroundThread<Pointer, Result<bool>>>,
  queue: Arc<DoubleBuffer<GcPointer>>,
  initialized: Arc<AtomicBool>,
  logger: LogFilter,
) -> impl Fn(Option<()>) -> Result {
  move |_| {
    if !initialized.load(Ordering::Acquire) {
      logger.debug("gc not ready for clean leaf nodes.");
      return Ok(());
    }

    logger.debug("clean leaf collect start.");

    let mut index = buffer_pool
      .peek(HEADER_INDEX)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    while let CursorNode::Internal(node) = buffer_pool
      .peek(index)?
      .for_read()
      .as_ref()
      .deserialize::<CursorNode>()?
    {
      index = node.first_child()
    }

    let mut index = Some(index);
    while let Some(i) = index.take() {
      {
        let leaf = buffer_pool
          .peek(i)?
          .for_read()
          .as_ref()
          .deserialize::<CursorNode>()?
          .as_leaf()?;
        if !check_c
          .send_batch(leaf.get_entry_pointers().collect())
          .wait()?
          .into_iter()
          .fold(Ok(false), |a, c| a.and_then(|a| c.map(|c| a || c)))?
        {
          index = leaf.get_next();
          continue;
        }
      }

      let mut slot = buffer_pool.peek(i)?.for_write();
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

      orphand
        .into_iter()
        .map(GcPointer::Release)
        .for_each(|p| queue.push(p));
    }

    logger.debug("clean leaf collect end.");
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
  release_c: Arc<dyn BackgroundThread<Pointer, Result>>,
) -> impl Fn(Pointer) -> Result {
  let buffer_pool = buffer_pool.clone();
  let release_c = release_c.clone();
  let version_visibility = version_visibility.clone();
  move |ptr: Pointer| {
    let mut index = Some(ptr);
    let mut max_found = false;

    while let Some(i) = index.take() {
      let mut slot = buffer_pool.peek(i)?.for_write();
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
        Some(i) => i,
        None => return recorder.serialize_and_log(0, &mut slot, &entry),
      };

      let next_entry: DataEntry =
        buffer_pool.peek(next)?.for_read().as_ref().deserialize()?;
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
        .peek(pointer)?
        .for_read()
        .as_ref()
        .deserialize::<DataEntry>()?
        .is_empty(),
    )
  }
}
