use std::{
  collections::{BTreeMap, BTreeSet, HashMap},
  fs::read_dir,
  mem::replace,
  path::PathBuf,
  sync::Arc,
};

use chrono::Local;

use super::{LogRecord, Operation, WALSegment, WAL_BLOCK_SIZE};
use crate::{
  constant::FILE_SUFFIX,
  disk::{DiskController, DiskControllerConfig, Page, PagePool},
  utils::logger,
  Error, Result,
};

pub fn replay(
  base_dir: &str,
  prefix: &str,
  max_len: usize,
  page_pool: Arc<PagePool<WAL_BLOCK_SIZE>>,
) -> Result<(
  usize,                          // last index
  usize,                          // last_log_id
  usize,                          // last_tx_id
  usize,                          // last_free
  BTreeSet<usize>,                // aborted
  Vec<(usize, usize, Page)>,      // redo records
  DiskController<WAL_BLOCK_SIZE>, // wal file controller
  Vec<WALSegment>,                // previous segments
)> {
  let mut files = Vec::new();
  for file in read_dir(base_dir).map_err(Error::IO)? {
    let file = file.map_err(Error::IO)?;
    if !file.file_name().to_string_lossy().starts_with(prefix) {
      continue;
    }
    files.push(file.path())
  }

  let file_prefix = PathBuf::from(base_dir).join(prefix);
  if files.len() == 0 {
    logger::info("previous wal segment not found.");
    return Ok((
      0,
      0,
      0,
      0,
      Default::default(),
      Default::default(),
      open_file(file_prefix, page_pool)?,
      Default::default(),
    ));
  }

  let mut tx_id = 0;
  let mut log_id = 0;
  let mut index = 0;
  let mut redo = BTreeMap::<usize, LogRecord>::new();

  let mut apply = HashMap::<usize, Vec<(usize, usize, Page)>>::new();
  let mut commited = Vec::<(usize, usize, Page)>::new();
  let mut f = None;
  let mut last_file = None;
  let mut segments = Vec::new();

  files.sort();
  for path in files.into_iter() {
    let wal = DiskController::open(
      DiskControllerConfig {
        path,
        thread_count: Some(3),
      },
      page_pool.clone(),
    )?;

    let len = wal.len()?;
    let mut records = vec![];

    for i in 0..len {
      for record in Vec::<LogRecord>::try_from(wal.read(i)?.as_ref())? {
        records.push((i, record))
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
          redo.split_off(&log_id);
          f = Some(free);
        }
        _ => drop(redo.insert(record.log_id, record)),
      };
    }

    if let Some(last) = replace(&mut last_file, Some(wal)) {
      segments.push(WALSegment::new(last));
    }
  }

  let mut free = f.map(|i| vec![i]).unwrap_or_default();
  for record in redo.into_values() {
    tx_id = tx_id.max(record.tx_id);
    match record.operation {
      Operation::Insert(i, page) => {
        if let Some(f) = free.last() {
          if *f == i {
            free.pop();
          }
        }
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
      Operation::Free(index) => {
        free.push(index);
      }
    };
  }
  let mut last_index = index + 1;
  if last_index == max_len {
    last_index = 0;
    last_file = Some(open_file(file_prefix, page_pool)?)
  }

  let wal = last_file.unwrap();
  let aborted = BTreeSet::from_iter(apply.into_keys());
  Ok((
    last_index,
    log_id + 1,
    tx_id + 1,
    free.last().map(|i| *i).unwrap_or(0),
    aborted,
    commited,
    wal,
    segments,
  ))
}

pub fn open_file(
  prefix: PathBuf,
  page_pool: Arc<PagePool<WAL_BLOCK_SIZE>>,
) -> Result<DiskController<WAL_BLOCK_SIZE>> {
  let config = DiskControllerConfig {
    path: format!(
      "{}{}{}",
      prefix.to_str().unwrap(),
      Local::now().timestamp_millis(),
      FILE_SUFFIX
    )
    .into(),
    thread_count: Some(3),
  };
  DiskController::open(config, page_pool)
}
