use std::{sync::Arc, time::Duration};

use lfkv_db::{Engine, EngineConfig};

fn main() {
  let engine = Arc::new(
    Engine::bootstrap(EngineConfig {
      base_path: "./.local",
      disk_batch_delay: Duration::from_millis(10),
      disk_batch_size: 100,
      wal_file_size: 16 << 20,
      checkpoint_interval: Duration::from_secs(30),
      group_commit_delay: Duration::from_millis(10),
      group_commit_count: 100,
      gc_trigger_count: 1000,
      gc_trigger_interval: Duration::from_secs(60),
      buffer_pool_shard_count: 1,
    })
    .unwrap(),
  );
  let mut v = vec![];
  for i in 0..2 {
    let e = engine.clone();
    let (tx, rx) = crossbeam::channel::unbounded();
    v.push(rx);
    std::thread::spawn(move || {
      let r = e.new_transaction().and_then(|mut t| {
        let vec = format!("123{}", i).as_bytes().to_vec();
        t.insert(vec.to_vec(), vec)?;
        t.commit()?;
        Ok(())
      });
      tx.send(r).unwrap();
    });
  }

  for r in v {
    if let Err(err) = r.recv().unwrap() {
      println!("{:?}", err)
    }
  }

  engine
    .new_transaction()
    .and_then(|mut c| {
      println!("{:?}", c.get(b"1230".to_vec().as_ref()).unwrap());
      c.commit()
    })
    .unwrap();
}
