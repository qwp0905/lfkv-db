use std::{
  hash::{BuildHasher, RandomState},
  ops::Div,
  path::PathBuf,
  sync::Mutex,
};

use crate::{
  buffer_pool::shard::{BufferPoolShard, CachedPage},
  disk::{DiskController, DiskControllerConfig, PagePool, PageRef, PAGE_SIZE},
  Result, ShortenedMutex, ToArc,
};

pub struct BufferPoolConfig {
  pub shard_count: usize,
  pub capacity: usize,
  pub path: PathBuf,
  pub read_threads: Option<usize>,
  pub write_threads: Option<usize>,
}

pub struct BufferPool {
  shards: Vec<Mutex<BufferPoolShard>>,
  hasher: RandomState,
}
impl BufferPool {
  pub fn open(config: BufferPoolConfig) -> Result<Self> {
    let dc = DiskControllerConfig {
      path: config.path,
      read_threads: config.read_threads,
      write_threads: config.write_threads,
    };
    let page_pool = PagePool::new(1).to_arc();
    let disk = DiskController::open(dc, page_pool)?.to_arc();
    let hasher = Default::default();
    let mut shards = Vec::with_capacity(config.shard_count);
    let cap = config.capacity.div(config.shard_count);
    for _ in 0..config.shard_count {
      shards.push(Mutex::new(BufferPoolShard::new(disk.clone(), cap)))
    }

    Ok(Self { shards, hasher })
  }

  #[inline]
  fn get_shard(&self, key: usize) -> (u64, &Mutex<BufferPoolShard>) {
    let h = self.hasher.hash_one(key);
    (h, &self.shards[h as usize % self.shards.len()])
  }

  pub fn read(&self, index: usize) -> Result<CachedPage<'_>> {
    let (h, shard) = self.get_shard(index);
    let mut s = shard.l();
    let (id, dirty, page) = s.get_mut(index, h, &self.hasher)?;
    let ptr = page as *mut PageRef<PAGE_SIZE>;
    Ok(CachedPage::new(s, ptr, id, dirty))
  }

  pub fn flush(&self) -> Result<()> {
    for shard in &self.shards {
      shard.l().flush()?;
    }
    Ok(())
  }
}
unsafe impl Send for BufferPool {}
unsafe impl Sync for BufferPool {}
