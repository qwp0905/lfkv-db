use std::{
  collections::VecDeque,
  sync::{Arc, RwLock},
};

use crossbeam::channel::{Receiver, Sender};
use raft::{
  eraftpb::{ConfState, Entry, HardState, Snapshot},
  Storage,
};
use utils::ShortRwLocker;

use crate::{
  error::to_raft_error,
  filesystem::{Page, PageSeeker},
  thread::ThreadPool,
};

use super::log::Header;

static HEADER_INDEX: usize = 0;

#[derive(Debug)]
pub struct WAL {
  core: RwLock<WALCore>,
}
impl Storage for WAL {
  fn term(&self, idx: u64) -> raft::Result<u64> {
    todo!();
  }
  fn entries(
    &self,
    low: u64,
    high: u64,
    max_size: impl Into<Option<u64>>,
    context: raft::GetEntriesContext,
  ) -> raft::Result<Vec<Entry>> {
    todo!();
  }
  fn first_index(&self) -> raft::Result<u64> {
    todo!();
  }
  fn initial_state(&self) -> raft::Result<raft::prelude::RaftState> {
    todo!();
  }
  fn last_index(&self) -> raft::Result<u64> {
    todo!();
  }
  fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
    todo!();
  }
}

impl WAL {
  pub fn apply_snapshot(&self, snapshot: Snapshot) {
    todo!()
  }

  pub fn set_hard_state(&self, hard_state: HardState) -> raft::Result<()> {
    todo!()
  }

  pub fn mut_hard_state(&self) -> &mut HardState {
    todo!()
  }

  pub fn set_conf_state(&self, conf_state: ConfState) -> raft::Result<()> {
    todo!()
  }

  pub fn commit(&self, entry: Entry) -> raft::Result<()> {
    let mut core = self.core.wl();
    if let Some(de) = core.buffer.remove(entry.get_index() as usize) {
      core.seeker.append(de.data.into()).map_err(to_raft_error)?;
      return core.seeker.fsync().map_err(to_raft_error);
    };
    Ok(())
  }
}

#[derive(Debug)]
pub struct WALCore {
  seeker: Arc<PageSeeker>,
  max_file_size: usize,
  buffer: VecDeque<Entry>,
  background: ThreadPool<std::io::Result<()>>,
  channel: Sender<Option<Receiver<Page>>>,
  commit_c: Receiver<Option<Receiver<Page>>>,
}
impl WALCore {
  fn start_background(&self) {
    // let seeker = Arc::clone(&self.seeker);
    // let recv = self.commit_c.to_owned();
    self.background.schedule(move || {
      // while let Ok(Some(rx)) = recv.recv() {
      //   let mut ws = seeker.l();
      //   let mut last: usize;
      //   while let Ok(page) = rx.recv() {
      //     last = ws.append(page)?;
      //   }
      // let header = Header(ws.get_page(0)?);
      //   ws.fsync()?;
      // }
      Ok(())
    });
  }

  fn get_header(&self) -> raft::Result<Header> {
    let page = self.seeker.read(HEADER_INDEX).map_err(to_raft_error)?;
    todo!()
  }

  pub fn apply_snapshot(&self, snapshot: Snapshot) {
    todo!()
  }

  pub fn set_hard_state(&self, hard_state: HardState) -> raft::Result<()> {
    todo!()
  }

  pub fn mut_hard_state(&self) -> &mut HardState {
    todo!()
  }

  pub fn set_conf_state(&self, conf_state: ConfState) -> raft::Result<()> {
    todo!()
  }

  pub fn commit(&mut self, entry: Entry) -> raft::Result<()> {
    if let Some(de) = self.buffer.remove(entry.get_index() as usize) {
      self.seeker.append(de.data.into()).map_err(to_raft_error)?;
      return self.seeker.fsync().map_err(to_raft_error);
    };
    Ok(())
  }
}
impl Drop for WALCore {
  fn drop(&mut self) {
    self.channel.send(None).unwrap();
  }
}
