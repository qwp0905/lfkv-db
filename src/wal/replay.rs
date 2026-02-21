use std::{
  collections::{BTreeMap, BTreeSet, HashMap},
  fs::read_dir,
  mem::replace,
  ops::Div,
  path::PathBuf,
  sync::Arc,
};

use chrono::Local;

use super::{LogRecord, Operation, WALSegment, WAL_BLOCK_SIZE};
use crate::{
  constant::FILE_SUFFIX,
  disk::{DiskController, DiskControllerConfig, Page, PagePool},
  Error, Result,
};

pub fn replay(
  base_dir: &str,
  prefix: &str,
  max_index: usize,
  page_pool: Arc<PagePool<WAL_BLOCK_SIZE>>,
) -> Result<(
  usize,
  usize,
  usize,
  usize,
  BTreeSet<usize>,
  Vec<(usize, usize, Page)>,
  DiskController<WAL_BLOCK_SIZE>,
  Vec<WALSegment>,
)> {
  let mut files = Vec::new();
  for file in read_dir(base_dir).map_err(Error::IO)? {
    let file = file.map_err(Error::IO)?;
    if !file.file_name().to_string_lossy().starts_with(prefix) {
      continue;
    }
    files.push(file.path())
  }
  if files.len() == 0 {
    return Ok((
      0,
      0,
      0,
      0,
      Default::default(),
      Default::default(),
      DiskController::open(
        DiskControllerConfig {
          path: PathBuf::from(base_dir)
            .join(prefix)
            .join(Local::now().to_string())
            .join(FILE_SUFFIX),
          read_threads: Some(1),
          write_threads: Some(3),
        },
        page_pool,
      )?,
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
        read_threads: Some(1),
        write_threads: Some(3),
      },
      page_pool.clone(),
    )?;

    let len = wal.len()?;
    let mut records = vec![];

    for i in 0..len.div(WAL_BLOCK_SIZE) {
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
  if last_index == max_index {
    last_index = 0;
    last_file = Some(DiskController::open(
      DiskControllerConfig {
        path: PathBuf::from(base_dir)
          .join(prefix)
          .join(Local::now().to_string()),
        read_threads: Some(1),
        write_threads: Some(3),
      },
      page_pool,
    )?)
  }

  let wal = last_file.unwrap();
  let aborted = BTreeSet::from_iter(apply.into_keys());
  Ok((
    last_index,
    log_id,
    tx_id,
    free.last().map(|i| *i).unwrap_or_default(),
    aborted,
    commited,
    wal,
    segments,
  ))
}
