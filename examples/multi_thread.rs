use std::{sync::Arc, time::Duration};

use lfkv_db::EngineBuilder;

fn main() {
  let engine = Arc::new(
    EngineBuilder::new("./.local")
      .group_commit_count(1000)
      .group_commit_delay(Duration::from_millis(10))
      .buffer_pool_memory_capacity(30 << 30)
      .buffer_pool_shard_count(1 << 6)
      .gc_trigger_count(300)
      .build()
      .expect("bootstrap error"),
  );
  let mut v = vec![];
  let count = 1000i64;

  for i in 0..count {
    let e = engine.clone();
    let (tx, rx) = crossbeam::channel::unbounded();
    v.push(rx);
    std::thread::Builder::new()
      .name(format!("{i}th thread"))
      .spawn(move || {
        let mut r = e.new_transaction().expect("start error");
        let vec = format!("123{}", i).as_bytes().to_vec();
        r.insert(vec.clone(), vec).expect("insert error");
        r.commit().expect("commit error");
        tx.send(()).unwrap();
      })
      .unwrap();
  }

  v.into_iter().for_each(|r| r.recv().unwrap());

  let mut t = engine.new_transaction().expect("scan start error");
  let mut iter = t.scan_all().expect("scan all error");
  while let Ok(Some((k, v))) = iter.try_next() {
    println!("{:?} {:?}", k, String::from_utf8_lossy(&v))
  }

  t.commit().expect("scan commit error");
}
