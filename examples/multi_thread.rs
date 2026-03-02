use std::{sync::Arc, time::Duration};

use crossbeam::channel::{unbounded, Sender};
use lfkv_db::EngineBuilder;

fn main() {
  let engine = Arc::new(
    EngineBuilder::new("./.local")
      .group_commit_count(256)
      .group_commit_delay(Duration::from_millis(10))
      .buffer_pool_memory_capacity(256 << 20)
      .buffer_pool_shard_count(1 << 4)
      .wal_file_size(16 << 20)
      .gc_thread_count(5)
      .io_thread_count(3)
      .build()
      .expect("bootstrap error"),
  );

  let count = 100_000_usize;
  let keys = (0..count)
    .map(|i| format!("123{}", i).as_bytes().to_vec())
    .collect::<Vec<Vec<u8>>>();

  let mut v = vec![];
  let threads_count = 100;
  let mut threads = Vec::new();
  let (tx, rx) = unbounded::<(Vec<u8>, Sender<()>)>();
  for i in 0..threads_count {
    let rx = rx.clone();
    let e = engine.clone();
    let th = std::thread::Builder::new()
      .name(format!("{i}th thread"))
      .stack_size(2 << 20)
      .spawn(move || {
        while let Ok((vec, t)) = rx.recv() {
          let mut r = e.new_transaction().expect("start error");
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
    tx.send((keys[i].clone(), t)).unwrap();
    v.push(r);
  }

  v.into_iter().for_each(|r| r.recv().unwrap());
  drop(tx);
  println!("{} ms", (chrono::Local::now() - start).num_milliseconds());
  for th in threads {
    let _ = th.join();
  }

  let mut total = 0;
  let mut found_eq = 0;
  let mut found_ne = 0;
  let mut not_found = 0;
  let mut t = engine.new_transaction().expect("scan start error");

  let mut iter = t.scan_all().expect("scan create error");
  while let Ok(Some(_)) = iter.try_next() {
    total += 1;
  }
  println!("total {}", total);
  for key in keys {
    match t.get(&key).unwrap() {
      Some(v) if v == key => found_eq += 1,
      Some(_) => found_ne += 1,
      None => not_found += 1,
    }
  }

  t.commit().expect("scan commit error");
  println!(
    "
  found and key equal: {found_eq}
  found and key not equal: {found_ne}
  not found: {not_found}"
  );

  drop(engine);
  println!("done");
}
