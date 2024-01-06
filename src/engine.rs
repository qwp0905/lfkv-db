use std::sync::{Arc, Mutex};

use crate::raft::Node;

pub struct Engine {
  node: Arc<Mutex<Node>>,
}

impl Engine {}
