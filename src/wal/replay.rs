use std::{
  collections::{BTreeMap, BTreeSet},
  fs::read_dir,
  mem::replace,
  sync::Arc,
  time::Duration,
};

use super::{Operation, WALSegment, WAL_BLOCK_SIZE};
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
  pub free_chain: BTreeSet<usize>,
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
      free_chain: Default::default(),
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
  let mut redo = BTreeMap::<usize, (usize, Page)>::new();
  let mut aborted = BTreeSet::new();

  let mut last_free = 0;
  let mut last_file = None;
  let mut segments = Vec::new();
  let mut free_logs = BTreeMap::<usize, usize>::new();
  let mut free_set = BTreeSet::new();

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
        Operation::Insert(i, page) => {
          redo.insert(record.log_id, (i, page));
          free_set.remove(&i);
        }
        Operation::Start => {}
        Operation::Commit => {}
        Operation::Abort => {
          aborted.insert(record.tx_id);
        }
        Operation::Checkpoint(free, last_log_id) => {
          redo = redo.split_off(&last_log_id);
          let removed = free_logs.split_off(&last_log_id);
          for (_, index) in replace(&mut free_logs, removed) {
            free_set.remove(&index);
          }
          last_free = free;
        }
        Operation::Free(free) => {
          free_logs.insert(record.log_id, free);
          free_set.insert(free);
        }
      };
    }

    if let Some(last) = replace(&mut last_file, Some(wal)) {
      segments.push(last);
    }
  }

  let mut last_index = index + 1;
  if last_index == max_len {
    last_index = 0;
    if let Some(last) = last_file.take() {
      segments.push(last);
    }
  }

  Ok(ReplayResult {
    last_index,
    last_log_id: log_id + 1,
    last_tx_id: tx_id + 1,
    last_free,
    free_chain: free_set,
    aborted,
    redo: redo
      .into_iter()
      .map(|(id, (index, data))| (id, index, data))
      .collect(),
    last_file,
    segments,
  })
}
