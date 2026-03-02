use std::{path::Path, time::Duration};

use crate::{Engine, EngineConfig, Result};

pub struct EngineBuilder<T>
where
  T: AsRef<Path>,
{
  base_path: T,
  wal_file_size: Option<usize>,
  checkpoint_interval: Option<Duration>,
  group_commit_delay: Option<Duration>,
  group_commit_count: Option<usize>,
  gc_trigger_interval: Option<Duration>,
  gc_thread_count: Option<usize>,
  buffer_pool_shard_count: Option<usize>,
  buffer_pool_memory_capacity: Option<usize>,
  io_thread_count: Option<usize>,
}
impl<T> EngineBuilder<T>
where
  T: AsRef<Path>,
{
  pub fn new(base_path: T) -> Self {
    Self {
      base_path,
      wal_file_size: None,
      checkpoint_interval: None,
      group_commit_delay: None,
      group_commit_count: None,
      gc_trigger_interval: None,
      gc_thread_count: None,
      buffer_pool_shard_count: None,
      buffer_pool_memory_capacity: None,
      io_thread_count: None,
    }
  }

  pub fn wal_file_size(mut self, size: usize) -> Self {
    self.wal_file_size = Some(size);
    self
  }
  pub fn checkpoint_interval(mut self, interval: Duration) -> Self {
    self.checkpoint_interval = Some(interval);
    self
  }
  pub fn group_commit_delay(mut self, delay: Duration) -> Self {
    self.group_commit_delay = Some(delay);
    self
  }
  pub fn group_commit_count(mut self, count: usize) -> Self {
    self.group_commit_count = Some(count);
    self
  }
  pub fn buffer_pool_shard_count(mut self, count: usize) -> Self {
    self.buffer_pool_shard_count = Some(count);
    self
  }
  pub fn buffer_pool_memory_capacity(mut self, capacity: usize) -> Self {
    self.buffer_pool_memory_capacity = Some(capacity);
    self
  }
  pub fn gc_trigger_interval(mut self, interval: Duration) -> Self {
    self.gc_trigger_interval = Some(interval);
    self
  }
  pub fn gc_thread_count(mut self, count: usize) -> Self {
    self.gc_thread_count = Some(count);
    self
  }
  pub fn io_thread_count(mut self, count: usize) -> Self {
    self.io_thread_count = Some(count);
    self
  }

  pub fn build(self) -> Result<Engine> {
    let config = EngineConfig {
      base_path: self.base_path,
      wal_file_size: self.wal_file_size.unwrap_or(DEFAULT_WAL_FILE_SIZE),
      checkpoint_interval: self
        .checkpoint_interval
        .unwrap_or(DEFAULT_CHECKPOINT_INTERVAL),
      group_commit_delay: self
        .group_commit_delay
        .unwrap_or(DEFAULT_GROUP_COMMIT_DELAY),
      group_commit_count: self
        .group_commit_count
        .unwrap_or(DEFAULT_GROUP_COMMIT_COUNT),
      gc_trigger_interval: self
        .gc_trigger_interval
        .unwrap_or(DEFAULT_GC_TRIGGER_INTERVAL),
      gc_thread_count: self.gc_thread_count.unwrap_or(DEFAULT_GC_THREAD_COUNT),
      buffer_pool_shard_count: self
        .buffer_pool_shard_count
        .unwrap_or(DEFAULT_BUFFER_POOL_SHARD_COUNT),
      buffer_pool_memory_capacity: self
        .buffer_pool_memory_capacity
        .unwrap_or(DEFAULT_BUFFER_POOL_MEMORY_CAPACITY),
      io_thread_count: self.io_thread_count.unwrap_or(DEFAULT_IO_THREAD_COUNT),
    };
    Engine::bootstrap(config)
  }
}

const DEFAULT_WAL_FILE_SIZE: usize = 8 << 20; // 8 mb
const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_GROUP_COMMIT_DELAY: Duration = Duration::from_millis(10);
const DEFAULT_GROUP_COMMIT_COUNT: usize = 100;
const DEFAULT_GC_TRIGGER_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_GC_THREAD_COUNT: usize = 3;
const DEFAULT_BUFFER_POOL_SHARD_COUNT: usize = 1 << 4; // 16
const DEFAULT_BUFFER_POOL_MEMORY_CAPACITY: usize = 32 << 20; // 32 mb
const DEFAULT_IO_THREAD_COUNT: usize = 3;
