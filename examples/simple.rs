use lfkv_db::{Engine, EngineConfig};
use std::time::Duration;

fn main() {
  let config = EngineConfig {
    base_path: "./.local",
    disk_batch_delay: Duration::from_millis(10),
    wal_file_size: 16 << 20,
    checkpoint_interval: Duration::from_secs(30),
    group_commit_delay: Duration::from_millis(10),
    group_commit_count: 100,
    gc_trigger_count: 1000,
    gc_trigger_interval: Duration::from_secs(60),
    buffer_pool_shard_count: 1,
    buffer_pool_memory_capacity: 100 << 20,
  };

  let engine = Engine::bootstrap(config).expect("bootstrap error");

  let mut w = engine.new_transaction().expect("write tx error");
  w.insert(b"123".to_vec(), b"123".to_vec())
    .expect("insert error");
  w.commit().expect("write commit error");

  let mut r = engine.new_transaction().expect("read tx error");
  println!("{:?}", r.get(&b"123".to_vec()).expect("find error"));
  r.commit().expect("read commit error");
}
