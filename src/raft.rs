use std::{
  collections::HashMap,
  time::{Duration, Instant},
};

use crossbeam::channel::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use protobuf::Message as Pbm;
use raft::{
  eraftpb::{ConfChange, ConfChangeV2, Entry, EntryType, Message},
  RaftLog, RawNode, StateRole, Storage,
};

use crate::{transport::Transport, wal::WAL};

pub struct Node {
  raw: RawNode<WAL>,
  own_mailbox: Receiver<Message>,
  transport: Transport,
  stop: Receiver<()>,
  proposed: Receiver<Proposal>,
  in_progress: HashMap<u64, Sender<bool>>,
}
impl Node {
  fn start(&mut self) -> Result<(), raft::Error> {
    let timeout = Duration::from_millis(10);
    let mut t = Instant::now();
    while let Err(RecvTimeoutError::Timeout) = self.stop.recv_timeout(timeout) {
      loop {
        match self.own_mailbox.try_recv() {
          Ok(msg) => {
            self.raw.step(msg)?;
          }
          Err(TryRecvError::Empty) => break,
          Err(TryRecvError::Disconnected) => return Ok(()),
        }
      }

      if t.elapsed() >= Duration::from_millis(100) {
        self.raw.tick();
        t = Instant::now();
      }

      if self.raw.is_leader() {
        while let Ok(proposal) = self.proposed.try_recv() {
          let index = proposal.entry.get_index();
          self.raw.handle_proposal(proposal.entry)?;
          self.in_progress.insert(index, proposal.done);
        }
      }

      self.on_ready()?;
    }

    Ok(())
  }

  fn handle_committed(&mut self, entries: Vec<Entry>) -> raft::Result<()> {
    for entry in entries {
      if entry.data.is_empty() {
        continue;
      }

      let index = entry.get_index();
      match entry.get_entry_type() {
        EntryType::EntryConfChange => {
          let mut cc = ConfChange::default();
          cc.merge_from_bytes(entry.get_data())?;
          let cs = self.raw.apply_conf_change(&cc)?;
          self.raw.store().set_conf_state(cs)?;
        }
        EntryType::EntryConfChangeV2 => {
          let mut cc = ConfChangeV2::default();
          cc.merge_from_bytes(entry.get_data())?;
          let cs = self.raw.apply_conf_change(&cc)?;
          self.raw.store().set_conf_state(cs)?;
        }
        EntryType::EntryNormal => {
          self.raw.store().commit(entry)?;
        }
      };
      if !self.raw.is_leader() {
        continue;
      }

      self
        .in_progress
        .remove(&index)
        .map(|c| c.send(true).unwrap());
    }
    Ok(())
  }

  fn on_ready(&mut self) -> raft::Result<()> {
    if !self.raw.has_ready() {
      return Ok(());
    }

    let mut ready = self.raw.ready();
    self.transport.propose(ready.take_messages())?;

    let snapshot = ready.snapshot();
    if !snapshot.is_empty() {
      self.raw.store().apply_snapshot(snapshot.to_owned());
    }
    self.handle_committed(ready.take_committed_entries())?;

    if let Some(hard_state) = ready.hs() {
      self.raw.store().set_hard_state(hard_state.to_owned())?;
    }
    self.transport.propose(ready.take_persisted_messages())?;

    let mut light = self.raw.advance(ready);
    if let Some(commit_index) = light.commit_index() {
      self.raw.store().mut_hard_state().set_commit(commit_index);
    }
    self.handle_committed(light.take_committed_entries())?;
    self.raw.advance_apply();
    Ok(())
  }
}

pub struct Proposal {
  entry: Entry,
  done: Sender<bool>,
}

trait RaftGroup<T: Storage> {
  fn raft_log(&self) -> &RaftLog<T>;
  fn store(&self) -> &T;
  fn is_leader(&self) -> bool;
  fn handle_proposal(&mut self, entry: Entry) -> raft::Result<()>;
}
impl<T> RaftGroup<T> for RawNode<T>
where
  T: Storage,
{
  fn raft_log(&self) -> &RaftLog<T> {
    &self.raft.raft_log
  }

  fn store(&self) -> &T {
    &self.raft.raft_log.store
  }

  fn is_leader(&self) -> bool {
    self.raft.state == StateRole::Leader
  }

  fn handle_proposal(&mut self, mut entry: Entry) -> raft::Result<()> {
    if entry.data.is_empty() {
      return Ok(());
    }

    let ctx: Vec<u8> = entry.take_context().into();
    match entry.get_entry_type() {
      EntryType::EntryConfChange => {
        let mut cc = ConfChange::default();
        cc.merge_from_bytes(entry.get_data())?;
        self.propose_conf_change(ctx, cc)
      }
      EntryType::EntryConfChangeV2 => {
        let mut cc = ConfChangeV2::default();
        cc.merge_from_bytes(entry.get_data())?;
        self.propose_conf_change(ctx, cc)
      }
      EntryType::EntryNormal => self.propose(ctx, entry.take_data().into()),
    }
  }
}
