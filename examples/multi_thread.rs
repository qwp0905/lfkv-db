use std::{sync::Arc, time::Duration};

use crossbeam::channel::{unbounded, Sender};
use lfkv_db::EngineBuilder;

fn main() {
  let engine = Arc::new(
    EngineBuilder::new("./.local")
      .group_commit_count(100)
      .group_commit_delay(Duration::from_millis(10))
      .buffer_pool_memory_capacity(100 << 20)
      .buffer_pool_shard_count(1 << 3)
      .wal_file_size(8 << 20)
      .gc_trigger_count(300)
      .gc_thread_count(5)
      .io_thread_count(5)
      .build()
      .expect("bootstrap error"),
  );
  let mut v = vec![];
  let count = 100_000_i64;

  let threads_count = 100;
  let mut threads = Vec::new();
  let (tx, rx) = unbounded::<(i64, Sender<()>)>();
  for i in 0..threads_count {
    let rx = rx.clone();
    let e = engine.clone();
    let th = std::thread::Builder::new()
      .name(format!("{i}th thread"))
      .stack_size(2 << 20)
      .spawn(move || {
        while let Ok((i, t)) = rx.recv() {
          let mut r = e.new_transaction().expect("start error");
          let vec = format!("123{}", i).as_bytes().to_vec();
          r.insert(vec.clone(), vec).expect("insert error");
          r.commit().expect("commit error");
          t.send(()).unwrap();
        }
      })
      .unwrap();
    threads.push(th)
  }

  let start = chrono::Local::now();
  for i in 0..count {
    let (t, r) = crossbeam::channel::unbounded();
    tx.send((i, t)).unwrap();
    v.push(r);
  }

  v.into_iter().for_each(|r| r.recv().unwrap());
  drop(tx);
  println!("{} ms", (chrono::Local::now() - start).num_milliseconds());
  for th in threads {
    let _ = th.join();
  }

  let mut c = 0;
  let mut t = engine.new_transaction().expect("scan start error");
  let mut iter = t.scan_all().expect("scan all error");
  while let Ok(Some(_)) = iter.try_next() {
    c += 1;
  }
  println!("total count: {c}");

  t.commit().expect("scan commit error");

  drop(engine);
  println!("done");
}
