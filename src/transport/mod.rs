mod client;
mod server;

use std::{collections::HashMap, sync::RwLock};

use crossbeam::channel::{unbounded, Receiver, Sender};
use raft::eraftpb::{ConfChange, ConfChangeType, ConfChangeV2, Message};
use utils::ShortRwLocker;

use self::client::GrpcClient;

#[derive(Debug)]
pub struct Peer {
  id: u64,
  sender: Sender<Option<Message>>,
}
impl Drop for Peer {
  fn drop(&mut self) {
    self.sender.send(None).unwrap();
  }
}

#[derive(Debug)]
struct Peers(RwLock<HashMap<u64, Peer>>);
impl Peers {
  fn send(&self, msg: Message) {
    let peers = self.0.rl();
    peers[&msg.to].sender.send(Some(msg)).unwrap();
  }

  fn add(&self, id: u64) -> Receiver<Option<Message>> {
    let mut peers = self.0.wl();
    let (tx, rx) = unbounded();
    peers.insert(id, Peer { id, sender: tx }).map(drop);
    return rx;
  }

  fn len(&self) -> usize {
    self.0.rl().len()
  }
}

pub struct Transport {
  peers: Peers,
  client: GrpcClient,
}
impl Transport {
  pub fn propose(&self, msgs: Vec<Message>) -> raft::Result<()> {
    for msg in msgs {
      self.peers.send(msg);
    }
    return Ok(());
  }

  fn add_peer(&self, id: u64, host: String) {
    let rx = self.peers.add(id);
    self.client.add_thread(self.peers.len());
    self.client.create_channel(host, rx);
  }

  pub fn apply_conf_change(&self, conf_change: &ConfChange) {
    match conf_change.change_type {
      ConfChangeType::AddNode => {
        self.add_peer(
          conf_change.get_node_id(),
          String::from_utf8_lossy(conf_change.get_context()).to_string(),
        );
      }
      ConfChangeType::RemoveNode => {}
      ConfChangeType::AddLearnerNode => {}
    }
  }

  pub fn apply_conf_change_v2(&self, conf_change: &ConfChangeV2) {
    for cc in conf_change.get_changes() {
      match cc.change_type {
        ConfChangeType::AddNode => {
          self.add_peer(
            cc.get_node_id(),
            String::from_utf8_lossy(conf_change.get_context()).to_string(),
          );
        }
        ConfChangeType::RemoveNode => {}
        ConfChangeType::AddLearnerNode => {}
      }
    }
  }
}
