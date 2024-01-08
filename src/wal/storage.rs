use std::{
  collections::VecDeque,
  sync::{Arc, RwLock},
};

use crossbeam::channel::{Receiver, Sender};
use utils::ShortRwLocker;

use crate::{
  filesystem::{Page, PageSeeker},
  thread::ThreadPool,
};

use super::log::Header;

static HEADER_INDEX: usize = 0;

pub struct WAL {
  core: RwLock<WALCore>,
}

pub struct WALCore {
  seeker: Arc<PageSeeker>,
  max_file_size: usize,
  background: ThreadPool<std::io::Result<()>>,
  channel: Sender<Option<Receiver<Page>>>,
  commit_c: Receiver<Option<Receiver<Page>>>,
}
