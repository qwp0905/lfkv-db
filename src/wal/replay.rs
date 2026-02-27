use std::{
  collections::{BTreeMap, BTreeSet, HashMap},
  fs::read_dir,
  mem::replace,
  path::PathBuf,
  sync::Arc,
  time::Duration,
};

use super::{LogRecord, Operation, WALSegment, WAL_BLOCK_SIZE};
use crate::{
  disk::{Page, PagePool},
  error::{Error, Result},
  utils::logger,
};

pub fn replay(
  base_dir: &str,
  prefix: &str,
  max_len: usize,
  flush_count: usize,
  flush_interval: Duration,
  page_pool: Arc<PagePool<WAL_BLOCK_SIZE>>,
) -> Result<(
  usize,                     // last index
  usize,                     // last_log_id
  usize,                     // last_tx_id
  usize,                     // last_free
  BTreeSet<usize>,           // aborted
  Vec<(usize, usize, Page)>, // redo records
  WALSegment,                // wal file controller
  Vec<WALSegment>,           // previous segments
)> {
  let mut files = Vec::new();
  for file in read_dir(base_dir).map_err(Error::IO)? {
    let file = file.map_err(Error::IO)?;
    if !file.file_name().to_string_lossy().starts_with(prefix) {
      continue;
    }
    files.push(file.path())
  }

  let file_prefix = &PathBuf::from(base_dir).join(prefix);
  if files.len() == 0 {
    logger::info("previous wal segment not found.");
    return Ok((
      0,
      0,
      0,
      0,
      Default::default(),
      Default::default(),
      WALSegment::open_new(file_prefix, flush_count, flush_interval)?,
      Default::default(),
    ));
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

    for i in 0..len {
      let mut page = page_pool.acquire();
      wal.read(i, &mut page)?;

      for record in Vec::<LogRecord>::try_from(page.as_ref())? {
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
    if let Some(last) = replace(
      &mut last_file,
      Some(WALSegment::open_new(
        file_prefix,
        flush_count,
        flush_interval,
      )?),
    ) {
      segments.push(last);
    }
  }
  let last_file = match last_file {
    Some(f) => f,
    None => WALSegment::open_new(file_prefix, flush_count, flush_interval)?,
  };

  let aborted = BTreeSet::from_iter(apply.into_keys());
  Ok((
    last_index,
    log_id + 1,
    tx_id + 1,
    last_free,
    aborted,
    commited,
    last_file,
    segments,
  ))
}
