use std::{
  collections::{BTreeMap, BTreeSet},
  fs::read_dir,
  sync::Arc,
  time::Duration,
};

use super::{Operation, WALSegment, WAL_BLOCK_SIZE};
use crate::{
  disk::{Page, PagePool},
  error::{Error, Result},
};

pub struct ReplayResult {
  pub last_log_id: usize,
  pub last_tx_id: usize,
  pub aborted: BTreeSet<usize>,
  pub redo: Vec<(usize, usize, Page)>,
  pub segments: Vec<WALSegment>,
}
impl ReplayResult {
  fn empty() -> Self {
    Self {
      last_log_id: 0,
      last_tx_id: 0,
      aborted: Default::default(),
      redo: Default::default(),
      segments: Default::default(),
    }
  }
}

pub fn replay(
  base_dir: &str,
  prefix: &str,
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
    return Ok(ReplayResult::empty());
  }

  let mut tx_id = 0;
  let mut log_id = 0;
  let mut redo = BTreeMap::<usize, (usize, Page)>::new();
  let mut aborted = BTreeSet::new();

  let mut segments = Vec::new();

  files.sort();
  for path in files.into_iter() {
    let wal = WALSegment::open_exists(path, flush_count, flush_interval)?; // only for replay. file number does not matter
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

    if let Some((_, record)) = records.last() {
      log_id = record.log_id;
    }

    for (_, record) in records {
      tx_id = tx_id.max(record.tx_id);
      match record.operation {
        Operation::Insert(i, page) => {
          redo.insert(record.log_id, (i, page));
        }
        Operation::Start => {}
        Operation::Commit => {}
        Operation::Abort => {
          aborted.insert(record.tx_id);
        }
        Operation::Checkpoint(last_log_id) => {
          redo = redo.split_off(&last_log_id);
        }
      };
    }

    segments.push(wal);
  }

  Ok(ReplayResult {
    last_log_id: log_id + 1,
    last_tx_id: tx_id + 1,
    aborted,
    redo: redo
      .into_iter()
      .map(|(id, (index, data))| (id, index, data))
      .collect(),
    segments,
  })
}
