use crossbeam::channel::{Receiver, Sender};
use raft::eraftpb::Message;

use crate::thread::ThreadPool;

#[derive(Debug)]
pub struct GrpcClient {
  threads: ThreadPool,
  mailbox: Sender<Message>,
}
impl GrpcClient {
  pub fn add_thread(&self, len: usize) {
    self.threads.scale_out(len);
  }

  pub fn create_channel(
    &self,
    host: String,
    channel: Receiver<Option<Message>>,
  ) {
    let mailbox = self.mailbox.to_owned();
    self.threads.schedule(move || {
      while let Ok(Some(msg)) = channel.recv() {
        host.to_string();
        todo!("send message to peer and process result");
        // mailbox.capacity();
      }
    })
  }
}
