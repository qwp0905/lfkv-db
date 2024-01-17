mod record;
pub use record::*;

mod wal;
pub use wal::*;
mod writer;
use crate::{
  logger,
  utils::{size, DroppableReceiver, ShortenedMutex, ShortenedRwLock},
  PAGE_SIZE,
};
use writer::*;

use std::{
  collections::BTreeMap,
  path::Path,
  sync::{Arc, Mutex, RwLock},
  time::Duration,
};

use crate::{
  disk::{Page, PageSeeker, Serializable},
  error::Result,
  thread::{ContextReceiver, StoppableChannel, ThreadPool},
};

// pub struct WAL {
//   core: Mutex<WALCore>,
// }
// impl WAL {
//   pub fn new<T>(
//     path: T,
//     flush_c: StoppableChannel<BTreeMap<usize, Page>>,
//     checkpoint_timeout: Duration,
//     max_log_size: usize,
//     max_buffer_size: usize,
//   ) -> Result<Self>
//   where
//     T: AsRef<Path>,
//   {
//     let seeker = PageSeeker::open(path)?;
//     if seeker.len()? == 0 {
//       let header = WALFileHeader::new(0, 0, 0);
//       seeker.write(HEADER_INDEX, header.serialize()?)?;
//       logger::info(format!("wal initialized"));
//     }

//     logger::info(format!("wal started"));
//     return Ok(Self {
//       core: Mutex::new(WALCore::new(
//         Arc::new(seeker),
//         max_log_size,
//         flush_c,
//         checkpoint_timeout,
//         max_buffer_size,
//       )),
//     });
//   }

//   pub fn append(
//     &self,
//     transaction_id: usize,
//     page_index: usize,
//     data: Page,
//   ) -> Result<()> {
//     let mut core = self.core.l();
//     core.append(transaction_id, page_index, data)
//   }

//   pub fn next_transaction(&self) -> Result<usize> {
//     let core = self.core.l();
//     core.next_transaction()
//   }

//   pub fn replay(&self) -> Result<()> {
//     self.core.l().replay()
//   }
// }

// pub struct WALCore {
//   seeker: Arc<PageSeeker>,
//   max_file_size: usize,
//   buffer: Arc<RwLock<Vec<WALRecord>>>,
//   background: ThreadPool<Result<()>>,
//   max_buffer_size: usize,
//   checkpoint_c: StoppableChannel<()>,
//   flush_c: StoppableChannel<BTreeMap<usize, Page>>,
// }
// impl WALCore {
//   fn new(
//     seeker: Arc<PageSeeker>,
//     max_file_size: usize,
//     flush_c: StoppableChannel<BTreeMap<usize, Page>>,
//     checkpoint_timeout: Duration,
//     max_buffer_size: usize,
//   ) -> Self {
//     let (checkpoint_c, recv) = StoppableChannel::new();
//     let core = Self {
//       seeker,
//       max_file_size: max_file_size / (PAGE_SIZE * 2),
//       buffer: Default::default(),
//       background: ThreadPool::new(2, size::mb(2), "wal", None),
//       max_buffer_size: max_buffer_size / (PAGE_SIZE * 2),
//       checkpoint_c,
//       flush_c,
//     };
//     core.start_background(recv, checkpoint_timeout);
//     return core;
//   }

//   fn start_background(&self, recv: ContextReceiver<()>, timeout: Duration) {
//     let seeker = self.seeker.clone();
//     let flush_c = self.flush_c.clone();
//     let buffer = self.buffer.clone();
//     self.background.schedule(move || {
//       while let Ok(_) = recv.recv_new_or_timeout(timeout) {
//         logger::info(format!("checkpoint triggered"));
//         let records: Vec<WALRecord> = { buffer.wl().drain(..).collect() };
//         let to_be_applied = match records.last() {
//           Some(e) => e.get_index(),
//           None => continue,
//         };

//         let entries = records
//           .into_iter()
//           .map(|entry| (entry.get_page_index(), entry.into()))
//           .collect();

//         flush_c.send_with_done(entries).drop_one();
//         let mut header: WALFileHeader =
//           seeker.read(HEADER_INDEX)?.deserialize()?;
//         header.applied = to_be_applied;
//         seeker.write(HEADER_INDEX, header.serialize()?)?;
//         seeker.fsync()?;
//         logger::info(format!("check point to {} done", header.applied));
//       }

//       logger::info(format!("wal background terminated"));
//       flush_c.terminate();
//       return Ok(());
//     })
//   }

//   fn append(
//     &mut self,
//     transaction_id: usize,
//     page_index: usize,
//     data: Page,
//   ) -> Result<()> {
//     let mut header: WALFileHeader =
//       self.seeker.read(HEADER_INDEX)?.deserialize()?;

//     let current = header.last_index + 1;
//     let record = WALRecord::new(current, transaction_id, page_index, data);
//     self.buffer.wl().push(record.clone());
//     let (record_header, record_data) = record.try_into()?;

//     let wi = ((current * 2) % self.max_file_size) + 1;
//     self.seeker.write(wi, record_header)?;
//     self.seeker.write(wi + 1, record_data)?;

//     header.last_index = current;
//     self.seeker.write(HEADER_INDEX, header.serialize()?)?;
//     self.seeker.fsync()?;
//     if self.buffer.rl().len() >= self.max_buffer_size {
//       self.checkpoint_c.send(());
//     }
//     return Ok(());
//   }

//   fn next_transaction(&self) -> Result<usize> {
//     let mut header: WALFileHeader =
//       self.seeker.read(HEADER_INDEX)?.deserialize()?;
//     let next = header.last_transaction + 1;
//     header.last_transaction = next;
//     self.seeker.write(HEADER_INDEX, header.serialize()?)?;
//     self.seeker.fsync()?;
//     return Ok(next);
//   }

//   fn replay(&self) -> Result<()> {
//     let mut header: WALFileHeader =
//       self.seeker.read(HEADER_INDEX)?.deserialize()?;
//     if header.last_index == header.applied {
//       logger::info(format!("nothing to replay in wal records"));
//       return Ok(());
//     };

//     logger::info(format!(
//       "last applied index {} will be {}",
//       header.applied, header.last_index
//     ));
//     while header.applied < header.last_index {
//       let mut entries = BTreeMap::new();
//       let end = (header.applied + self.max_buffer_size).min(header.last_index);
//       for index in header.applied..end {
//         let i = ((index * 2) % self.max_file_size) + 1;
//         let h = self.seeker.read(i)?.deserialize()?;
//         let p = self.seeker.read(i + 1)?;
//         let record = WALRecord::try_from((h, p))?;
//         if record.get_index() != index {
//           continue;
//         }
//         entries.insert(record.get_page_index(), record.into());
//       }

//       let rx = self.flush_c.send_with_done(entries);
//       rx.drop_one();
//       header.applied = end;
//       self.seeker.write(HEADER_INDEX, header.serialize()?)?;
//       self.seeker.fsync()?;
//     }
//     return Ok(());
//   }
// }
// impl Drop for WALCore {
//   fn drop(&mut self) {
//     self.checkpoint_c.send(());
//     self.checkpoint_c.terminate()
//   }
// }
