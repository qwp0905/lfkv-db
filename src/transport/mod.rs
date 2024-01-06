mod connection;

use std::collections::HashMap;

use crossbeam::channel::Sender;
use raft::eraftpb::Message;

#[derive(Debug)]
pub struct Peer {
  id: u64,
  host: String,
  sender: Sender<Message>,
}

#[derive(Debug)]
pub struct Transport {
  mailbox: Sender<Message>,
  peers: HashMap<u64, Peer>,
}
impl Transport {
  pub fn propose(&self, msgs: Vec<Message>) -> raft::Result<()> {
    // for msg in msgs {
    //   self.peers[&msg.to].send()
    // }
    Ok(())
  }
}
