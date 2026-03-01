use std::{
  collections::{BTreeMap, BTreeSet, HashMap},
  fs::read_dir,
  mem::replace,
  sync::Arc,
  time::Duration,
};

use super::{LogRecord, Operation, WALSegment, WAL_BLOCK_SIZE};
use crate::{
  disk::{Page, PagePool},
  error::{Error, Result},
  utils::logger,
};

pub struct ReplayResult {
  pub last_index: usize,
  pub last_log_id: usize,
  pub last_tx_id: usize,
  pub last_free: usize,
  pub aborted: BTreeSet<usize>,
  pub redo: Vec<(usize, usize, Page)>,
  pub last_file: Option<WALSegment>,
  pub segments: Vec<WALSegment>,
}
impl ReplayResult {
  fn empty() -> Self {
    Self {
      last_index: 0,
      last_log_id: 0,
      last_tx_id: 0,
      last_free: 0,
      aborted: Default::default(),
      redo: Default::default(),
      last_file: None,
      segments: Default::default(),
    }
  }
}

pub fn replay(
  base_dir: &str,
  prefix: &str,
  max_len: usize,
  flush_count: usize,
  flush_interval: Duration,
  page_pool: Arc<PagePool<WAL_BLOCK_SIZE>>,
) -> Result<ReplayResult> {
  let mut files = Vec::new();
  for file in read_dir(base_dir).map_err(Error::IO)? {
    let file = file.map_err(Error::IO)?;
    if !file.file_name().to_string_lossy().starts_with(prefix) {
      continue;
    }
    files.push(file.path())
  }

  if files.len() == 0 {
    logger::info("previous wal segment not found.");
    return Ok(ReplayResult::empty());
  }

  let mut tx_id = 0;
  let mut log_id = 0;
  let mut index = 0;
  let mut redo = BTreeMap::<usize, LogRecord>::new();

  let mut apply = HashMap::<usize, Vec<(usize, usize, Page)>>::new();
  let mut commited = Vec::<(usize, usize, Page)>::new();
  let mut last_free = 0;
  let mut last_file = None;
  let mut segments = Vec::new();

  files.sort();
  for path in files.into_iter() {
    let wal = WALSegment::open_exists(path, flush_count, flush_interval)?;
    let len = wal.len()?;
    let mut records = vec![];

    for i in 0..=len {
      let mut page = page_pool.acquire();
      wal.read(i, &mut page)?;

      let (r, complete) = page.as_ref().into();
      records.extend(r.into_iter().map(|record| (i, record)));
      if complete {
        break;
      }
    }
    records.sort_by_key(|(_, r)| r.log_id);

    if let Some((i, record)) = records.last() {
      index = *i;
      log_id = record.log_id;
    }

    for (_, record) in records {
      tx_id = tx_id.max(record.tx_id);
      match record.operation {
        Operation::Checkpoint(free, log_id) => {
          redo = redo.split_off(&log_id);
          last_free = free;
        }
        _ => drop(redo.insert(record.log_id, record)),
      };
    }

    if let Some(last) = replace(&mut last_file, Some(wal)) {
      segments.push(last);
    }
  }

  for record in redo.into_values() {
    tx_id = tx_id.max(record.tx_id);
    match record.operation {
      Operation::Insert(i, page) => {
        apply
          .entry(record.tx_id)
          .or_default()
          .push((record.log_id, i, page));
      }
      Operation::Start => {
        apply.insert(record.tx_id, vec![]);
      }
      Operation::Commit => {
        apply
          .remove(&record.tx_id)
          .map(|pages| commited.extend(pages));
      }
      Operation::Abort => {
        apply.remove(&record.tx_id);
      }
      Operation::Checkpoint(_, _) => {}
      Operation::Free(index) => last_free = index,
    };
  }
  let mut last_index = index + 1;
  if last_index == max_len {
    last_index = 0;
    if let Some(last) = last_file.take() {
      segments.push(last);
    }
  }

  let aborted = BTreeSet::from_iter(apply.into_keys());
  Ok(ReplayResult {
    last_index,
    last_log_id: log_id + 1,
    last_tx_id: tx_id + 1,
    last_free,
    aborted,
    redo: commited,
    last_file,
    segments,
  })
}
