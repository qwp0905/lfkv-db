use std::{sync::Arc, time::Duration};

use super::{CursorNode, DataEntry, Pointer, TreeHeader, HEADER_INDEX};
use crate::{
  error::Result,
  serialize::SerializeFrom,
  thread::{SafeWork, SendAll, SharedWorkThread, SingleWorkThread, WorkBuilder},
  transaction::TxOrchestrator,
  utils::ToArc,
};

pub struct GarbageCollector {
  main: SingleWorkThread<(), Result>,
  check: Arc<SharedWorkThread<Pointer, Result<bool>>>,
  entry: Arc<SharedWorkThread<Pointer, Result>>,
  release: Arc<SharedWorkThread<Pointer, Result>>,
}
impl GarbageCollector {
  pub fn start(orchestrator: Arc<TxOrchestrator>, timeout: Duration) -> Self {
    let release = WorkBuilder::new()
      .name("gc release entry")
      .stack_size(1)
      .shared(1)
      .build_unchecked(Self::run_release(orchestrator.clone()))
      .to_arc();
    let entry = WorkBuilder::new()
      .name("gc found entry")
      .stack_size(1)
      .shared(1)
      .build_unchecked(Self::run_entry(orchestrator.clone(), release.clone()))
      .to_arc();
    let check = WorkBuilder::new()
      .name("gc check top entry")
      .stack_size(1)
      .shared(1)
      .build_unchecked(Self::run_check(
        orchestrator.clone(),
        entry.clone(),
        release.clone(),
      ))
      .to_arc();
    let main = WorkBuilder::new()
      .name("gc main")
      .stack_size(1)
      .single()
      .with_timeout(timeout, Self::run_main(orchestrator.clone(), check.clone()));
    Self {
      main,
      check,
      entry,
      release,
    }
  }
  fn run_main(
    orchestrator: Arc<TxOrchestrator>,
    check_c: Arc<SharedWorkThread<Pointer, Result<bool>>>,
  ) -> impl Fn(Option<()>) -> Result {
    move |_| {
      let header: TreeHeader = orchestrator
        .fetch(HEADER_INDEX)?
        .for_read()
        .as_ref()
        .deserialize()?;

      let mut index = header.get_root();
      while let CursorNode::Internal(node) = orchestrator
        .fetch(index)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
      {
        index = node.first_child()
      }

      let mut index = Some(index);
      while let Some(i) = index.take() {
        let leaf = orchestrator
          .fetch(i)?
          .for_read()
          .as_ref()
          .deserialize::<CursorNode>()?
          .as_leaf()?;

        index = leaf.get_next();
        let mut found = false;
        for r in leaf.get_entries().map(|(_, p)| *p).send_all_to(&check_c)? {
          if r? && !found {
            found = true
          }
        }
        if !found {
          continue;
        }

        let released = {
          let mut latch = orchestrator.fetch(i)?.for_write();
          let mut leaf = latch.as_ref().deserialize::<CursorNode>()?.as_leaf()?;
          let mut new_entries = vec![];
          let mut release = vec![];
          for (key, ptr) in leaf.drain() {
            if !orchestrator
              .fetch(ptr)?
              .for_read()
              .as_ref()
              .deserialize::<DataEntry>()?
              .is_empty()
            {
              new_entries.push((key, ptr));
              continue;
            };
            release.push(ptr);
          }

          if !release.is_empty() {
            leaf.set_entries(new_entries);
            latch.as_mut().serialize_from(&CursorNode::Leaf(leaf))?;
          }
          release
        };

        for i in released {
          orchestrator.release(i)?;
        }
      }

      Ok(())
    }
  }
  fn run_release(
    orchestrator: Arc<TxOrchestrator>,
  ) -> impl Fn(usize) -> SafeWork<Pointer, Result> {
    move |_| {
      let orchestrator = orchestrator.clone();
      SafeWork::no_timeout(move |pointer: Pointer| {
        let mut p = Some(pointer);

        while let Some(i) = p.take() {
          p = orchestrator
            .fetch(i)?
            .for_read()
            .as_ref()
            .deserialize::<DataEntry>()?
            .get_next();
          orchestrator.release(i)?;
        }
        Ok(())
      })
    }
  }
  fn run_entry(
    orchestrator: Arc<TxOrchestrator>,
    release_c: Arc<SharedWorkThread<Pointer, Result>>,
  ) -> impl Fn(usize) -> SafeWork<Pointer, Result> {
    move |_| {
      let orchestrator = orchestrator.clone();
      let release_c = release_c.clone();
      let work = SafeWork::no_timeout(move |pointer: Pointer| {
        let mut index = Some(pointer);
        while let Some(i) = index.take() {
          let mut latch = orchestrator.fetch(i)?.for_write();
          let mut entry = latch.as_ref().deserialize::<DataEntry>()?;
          let next = entry.get_next();

          if !entry.remove_until(orchestrator.min_version()) {
            index = next;
            continue;
          }
          if let Some(n) = next {
            release_c.send_no_wait(n);
          }
          latch.as_mut().serialize_from(&entry)?;

          break;
        }
        Ok(())
      });
      work
    }
  }

  fn run_check(
    orchestrator: Arc<TxOrchestrator>,
    entry_c: Arc<SharedWorkThread<Pointer, Result>>,
    release_c: Arc<SharedWorkThread<Pointer, Result>>,
  ) -> impl Fn(usize) -> SafeWork<Pointer, Result<bool>> {
    move |_| {
      let orchestrator = orchestrator.clone();
      let entry_c = entry_c.clone();
      let release_c = release_c.clone();
      let work = SafeWork::no_timeout(move |pointer: Pointer| {
        let mut latch = orchestrator.fetch(pointer)?.for_write();
        let mut entry = latch.as_ref().deserialize::<DataEntry>()?;
        let next = entry.get_next();
        let (result, c) = match entry.remove_until(orchestrator.min_version()) {
          true => (true, &release_c),
          false => (false, &entry_c),
        };
        if result {
          latch.as_mut().serialize_from(&entry)?;
        }
        if let Some(i) = next {
          c.send_no_wait(i);
        }
        Ok(result)
      });
      work
    }
  }
}
