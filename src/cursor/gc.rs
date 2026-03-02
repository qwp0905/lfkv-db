use std::{
  sync::{Arc, Mutex},
  time::Duration,
};

use super::{CursorNode, DataEntry, Pointer, TreeHeader, HEADER_INDEX};
use crate::{
  buffer_pool::BufferPool,
  error::Result,
  serialize::SerializeFrom,
  thread::{SharedWorkThread, SingleWorkThread, WorkBuilder},
  transaction::{FreeList, VersionVisibility},
  utils::{ShortenedMutex, ToArc},
  wal::WAL,
};

pub struct GarbageCollectionConfig {
  pub interval: Duration,
  pub count: usize,
  pub thread_count: usize,
}

pub struct GarbageCollector {
  main: SingleWorkThread<(), Result>,
  check: Arc<SharedWorkThread<Pointer, Result<bool>>>,
  entry: Arc<SharedWorkThread<Pointer, Result>>,
  release: Arc<SharedWorkThread<Pointer, Result>>,
  count: Arc<Mutex<usize>>,
  max_count: usize,
}
impl GarbageCollector {
  pub fn notify(&self) {
    let mut c = self.count.l();
    if *c != self.max_count {
      *c += 1;
      return;
    }
    self.main.send_no_wait(());
  }

  pub fn run(&self) -> Result {
    *self.count.l() = 0;
    self.main.send_await(())?
  }

  pub fn start(
    buffer_pool: Arc<BufferPool>,
    version_visibility: Arc<VersionVisibility>,
    free_list: Arc<FreeList>,
    wal: Arc<WAL>,
    config: GarbageCollectionConfig,
  ) -> Self {
    let release = WorkBuilder::new()
      .name("gc release entry")
      .stack_size(2 << 20)
      .shared(config.thread_count)
      .build_unchecked(run_release(buffer_pool.clone(), free_list.clone()))
      .to_arc();
    let entry = WorkBuilder::new()
      .name("gc found entry")
      .stack_size(2 << 20)
      .shared(config.thread_count)
      .build_unchecked(run_entry(
        buffer_pool.clone(),
        version_visibility.clone(),
        wal.clone(),
        release.clone(),
      ))
      .to_arc();
    let check = WorkBuilder::new()
      .name("gc check top entry")
      .stack_size(2 << 20)
      .shared(config.thread_count)
      .build_unchecked(run_check(
        buffer_pool.clone(),
        version_visibility.clone(),
        wal.clone(),
        entry.clone(),
        release.clone(),
      ))
      .to_arc();
    let count: Arc<Mutex<usize>> = Default::default();
    let main = WorkBuilder::new()
      .name("gc main")
      .stack_size(2 << 20)
      .single()
      .with_timeout(
        config.interval,
        run_main(
          buffer_pool,
          version_visibility,
          wal,
          check.clone(),
          release.clone(),
          count.clone(),
        ),
      );
    Self {
      main,
      check,
      entry,
      release,
      max_count: config.count,
      count,
    }
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
  wal: Arc<WAL>,
  check_c: Arc<SharedWorkThread<Pointer, Result<bool>>>,
  release_c: Arc<SharedWorkThread<Pointer, Result>>,
  counter: Arc<Mutex<usize>>,
) -> impl Fn(Option<()>) -> Result {
  move |_| {
    *counter.l() = 0;

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
      let leaf = buffer_pool
        .read(i)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
        .as_leaf()?;

      index = leaf.get_next();
      let mut found = false;
      let mut waiting = Vec::with_capacity(leaf.len());
      for (_, ptr) in leaf.get_entries() {
        waiting.push(check_c.send(*ptr));
      }
      for r in waiting {
        found = r.wait_flatten()? || found;
      }
      if !found {
        continue;
      }

      let mut latch = buffer_pool.read(i)?.for_write();
      let mut leaf = latch.as_ref().deserialize::<CursorNode>()?.as_leaf()?;
      let prev_len = leaf.len();
      let mut new_entries = vec![];
      for (key, ptr) in leaf.drain() {
        if !buffer_pool
          .read(ptr)?
          .for_read()
          .as_ref()
          .deserialize::<DataEntry>()?
          .is_empty()
        {
          new_entries.push((key, ptr));
          continue;
        };
        release_c.send_no_wait(ptr);
      }

      if new_entries.len() == prev_len {
        continue;
      }

      leaf.set_entries(new_entries);
      latch.as_mut().serialize_from(&CursorNode::Leaf(leaf))?;
      wal.append_insert(0, latch.get_index(), latch.as_ref())?;
    }

    version_visibility.remove_aborted(&min_version);
    Ok(())
  }
}

fn run_release(
  buffer_pool: Arc<BufferPool>,
  free_list: Arc<FreeList>,
) -> impl Fn(Pointer) -> Result {
  let orchestrator = buffer_pool.clone();
  let free_list = free_list.clone();
  move |pointer: Pointer| {
    let mut p = Some(pointer);

    while let Some(i) = p.take() {
      p = orchestrator
        .read(i)?
        .for_read()
        .as_ref()
        .deserialize::<DataEntry>()?
        .get_next();
      free_list.release(i)?;
    }
    Ok(())
  }
}

fn run_entry(
  buffer_pool: Arc<BufferPool>,
  version_visibility: Arc<VersionVisibility>,
  wal: Arc<WAL>,
  release_c: Arc<SharedWorkThread<Pointer, Result>>,
) -> impl Fn(Pointer) -> Result {
  let buffer_pool = buffer_pool.clone();
  let release_c = release_c.clone();
  let version_visibility = version_visibility.clone();
  move |pointer: Pointer| {
    let mut index = Some(pointer);
    while let Some(i) = index.take() {
      let mut latch = buffer_pool.read(i)?.for_write();
      let mut entry = latch.as_ref().deserialize::<DataEntry>()?;
      let next = entry.get_next();

      let expired = entry.remove_until(version_visibility.min_version());
      let modified = entry.filter_aborted(|v| version_visibility.is_aborted(v));
      if expired || modified {
        latch.as_mut().serialize_from(&entry)?;
        wal.append_insert(0, latch.get_index(), latch.as_ref())?;
      }
      if !expired {
        index = next;
        continue;
      }
      if let Some(n) = next {
        release_c.send_no_wait(n);
      }
      break;
    }
    Ok(())
  }
}

fn run_check(
  buffer_pool: Arc<BufferPool>,
  version_visibility: Arc<VersionVisibility>,
  wal: Arc<WAL>,
  entry_c: Arc<SharedWorkThread<Pointer, Result>>,
  release_c: Arc<SharedWorkThread<Pointer, Result>>,
) -> impl Fn(Pointer) -> Result<bool> {
  let buffer_pool = buffer_pool.clone();
  let version_visibility = version_visibility.clone();
  let entry_c = entry_c.clone();
  let release_c = release_c.clone();
  move |pointer: Pointer| {
    let mut latch = buffer_pool.read(pointer)?.for_write();
    let mut entry = latch.as_ref().deserialize::<DataEntry>()?;
    let next = entry.get_next();

    let expired = entry.remove_until(version_visibility.min_version());
    let modified = entry.filter_aborted(|v| version_visibility.is_aborted(v));

    let result = modified || expired;

    let c = (!result).then_some(&entry_c).unwrap_or(&release_c);
    if result {
      latch.as_mut().serialize_from(&entry)?;
      wal.append_insert(0, latch.get_index(), latch.as_ref())?;
    }
    if let Some(i) = next {
      c.send_no_wait(i);
    }
    Ok(entry.is_empty())
  }
}
