use std::{
  collections::HashSet,
  mem::replace,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, RwLock,
  },
  time::Duration,
};

use crate::{
  buffer_pool::{BufferPool, BufferPoolConfig, PageSlot, PageSlotWrite},
  error::{Error, Result},
  serialize::SerializeFrom,
  thread::{SingleWorkThread, WorkBuilder},
  transaction::FreePage,
  utils::{ShortenedRwLock, ToArc, ToArcRwLock},
  wal::{WALConfig, WAL},
};

struct VersionVisibility {
  active: HashSet<usize>,
  aborted: HashSet<usize>,
}
impl VersionVisibility {
  fn new() -> Self {
    Self {
      active: Default::default(),
      aborted: Default::default(),
    }
  }
}

pub struct TxOrchestrator {
  wal: Arc<WAL>,
  buffer_pool: Arc<BufferPool>,
  checkpoint: SingleWorkThread<(), Result>,
  last_free: Arc<RwLock<usize>>,
  version_visibility: Arc<RwLock<VersionVisibility>>,
  last_tx_id: AtomicUsize,
}
impl TxOrchestrator {
  pub fn new(
    buffer_pool_config: BufferPoolConfig,
    wal_config: WALConfig,
  ) -> Result<Self> {
    let buffer_pool = BufferPool::open(buffer_pool_config)?.to_arc();
    let (wal, last_tx_id, last_free, redo) = WAL::replay(wal_config)?;
    let wal = wal.to_arc();
    let last_free = last_free.to_arc_rwlock();
    for (_, i, page) in redo {
      buffer_pool
        .read(i)?
        .for_write()
        .as_mut()
        .writer()
        .write(page.as_ref())?;
    }

    let checkpoint = WorkBuilder::new()
      .name("")
      .stack_size(1)
      .single()
      .with_timeout(
        Duration::new(1, 1),
        handle_checkpoint(wal.clone(), buffer_pool.clone(), last_free.clone()),
      );

    Ok(Self {
      checkpoint,
      last_tx_id: AtomicUsize::new(last_tx_id),
      last_free,
      wal,
      buffer_pool,
      version_visibility: VersionVisibility::new().to_arc_rwlock(),
    })
  }
  pub fn fetch(&self, index: usize) -> Result<PageSlot<'_>> {
    self.buffer_pool.read(index)
  }

  pub fn log(&self, tx_id: usize, page: &PageSlotWrite<'_>) -> Result {
    let index = page.get_index();
    match self.wal.append_insert(tx_id, index, page.as_ref()) {
      Err(Error::WALCapacityExceeded) => self.checkpoint.send_await(())??,
      result => return result,
    };
    self.wal.append_insert(tx_id, index, page.as_ref())
  }

  pub fn alloc(&self) -> Result<PageSlot<'_>> {
    let mut index = self.last_free.wl();
    let slot = self.buffer_pool.read(*index)?;
    let next = slot
      .for_read()
      .as_ref()
      .deserialize::<FreePage>()?
      .get_next();
    match self.wal.append_free(next) {
      Ok(_) => {}
      Err(Error::WALCapacityExceeded) => self
        .checkpoint
        .send_await(())?
        .and_then(|_| self.wal.append_free(next))?,
      Err(err) => return Err(err),
    };
    *index = next;
    Ok(slot)
  }
  pub fn release(&self, page: PageSlot<'_>) -> Result {
    let index = page.get_index();
    let mut prev = self.last_free.wl();
    match self.wal.append_free(index) {
      Ok(_) => {}
      Err(Error::WALCapacityExceeded) => self
        .checkpoint
        .send_await(())?
        .and_then(|_| self.wal.append_free(index))?,
      Err(err) => return Err(err),
    };
    let free = FreePage::new(replace(&mut prev, index));
    page.for_write().as_mut().serialize_from(&free)?;
    Ok(())
  }

  pub fn start_tx(&self) -> Result<usize> {
    let tx_id = self.last_tx_id.fetch_add(1, Ordering::Release);
    match self.wal.append_start(tx_id) {
      Err(Error::WALCapacityExceeded) => self.checkpoint.send_await(())??,
      _ => return Ok(tx_id),
    }
    self.wal.append_start(tx_id)?;
    Ok(tx_id)
  }

  pub fn commit_tx(&self, tx_id: usize) -> Result {
    match self.wal.append_commit(tx_id) {
      Ok(_) => {}
      Err(Error::WALCapacityExceeded) => self
        .checkpoint
        .send_await(())?
        .and_then(|_| self.wal.append_commit(tx_id))?,
      Err(err) => return Err(err),
    }
    self.wal.flush()?;
    self.version_visibility.wl().active.remove(&tx_id);
    Ok(())
  }

  pub fn abort_tx(&self, tx_id: usize) -> Result {
    match self.wal.append_abort(tx_id) {
      Err(Error::WALCapacityExceeded) => self.checkpoint.send_await(())??,
      result => return result,
    }
    self.wal.append_abort(tx_id)?;
    let mut v = self.version_visibility.wl();
    if v.active.remove(&tx_id) {
      v.aborted.insert(tx_id);
    }
    Ok(())
  }

  pub fn is_visible(&self, tx_id: &usize) -> bool {
    let v = self.version_visibility.rl();
    !v.aborted.contains(tx_id) && !v.active.contains(tx_id)
  }

  pub fn is_active(&self, tx_id: &usize) -> bool {
    let v = self.version_visibility.rl();
    v.active.contains(tx_id)
  }

  pub fn current_version(&self) -> usize {
    self.last_tx_id.load(Ordering::Acquire)
  }
}

fn handle_checkpoint(
  wal: Arc<WAL>,
  buffer_pool: Arc<BufferPool>,
  last_free: Arc<RwLock<usize>>,
) -> impl Fn(Option<()>) -> Result {
  move |_| {
    buffer_pool.flush()?;
    match wal.append_checkpoint(*last_free.rl()) {
      Ok(_) => {}
      Err(Error::WALCapacityExceeded) => {}
      Err(err) => return Err(err),
    };
    wal.flush()
  }
}
