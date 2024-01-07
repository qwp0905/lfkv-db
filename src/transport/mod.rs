mod client;
mod server;

use std::{collections::HashMap, sync::RwLock};

use crossbeam::channel::{unbounded, Receiver, Sender};
use raft::eraftpb::Message;
use utils::ShortRwLocker;

use self::client::GrpcClient;

#[derive(Debug)]
pub struct Peer {
  id: u64,
  sender: Sender<Option<Message>>,
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
    peers.insert(id, Peer { id, sender: tx }).map(|old| {
      old.sender.send(None).unwrap();
    });
    return rx;
  }

  fn len(&self) -> usize {
    self.0.rl().len()
  }
}

#[derive(Debug)]
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

  pub fn add_peer(&self, id: u64, host: String) {
    let rx = self.peers.add(id);
    self.client.add_thread(self.peers.len());
    self.client.create_channel(host, rx);
  }
}
