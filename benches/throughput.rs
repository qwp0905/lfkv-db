use std::{sync::Arc, time::Duration};

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use crossbeam::channel::{unbounded, Sender};
use lfkv_db::EngineBuilder;
use tempfile::TempDir;

fn build<T: AsRef<std::path::Path> + ?Sized>(dir: &T) -> EngineBuilder<&T> {
  EngineBuilder::new(dir)
    .group_commit_count(512)
    .group_commit_delay(Duration::from_millis(10))
    .buffer_pool_memory_capacity(512 << 20)
    .buffer_pool_shard_count(1 << 8)
    .wal_file_size(32 << 20)
    .gc_thread_count(5)
    .io_thread_count(5)
}

fn pre_write_keys<
  'a,
  T: Iterator<Item = &'a Vec<u8>>,
  P: AsRef<std::path::Path> + ?Sized,
>(
  dir: &P,
  keys: T,
) {
  let engine = build(dir).build().unwrap();
  let mut tx = engine.new_transaction().unwrap();
  for k in keys {
    tx.insert(k.clone(), k.clone()).unwrap();
  }
  tx.commit().unwrap();
}

fn bench_sequential_insert(c: &mut Criterion) {
  const SIZE: usize = 1_000;
  let keys = (0..SIZE)
    .map(|i| format!("{i:06}").as_bytes().to_vec())
    .collect::<Vec<_>>();

  let mut group = c.benchmark_group("sequential-insert");
  group
    .sample_size(10)
    .measurement_time(Duration::from_secs(30))
    .throughput(Throughput::Elements(SIZE as u64))
    .bench_function("bench", |b| {
      b.iter_batched_ref(
        || {
          let dir = TempDir::new_in(".").expect("dir failed.");
          let engine = build(dir.path())
            .group_commit_count(1)
            .build()
            .expect("bootstrap error");
          (dir, engine)
        },
        |(_, engine)| {
          for i in 0..SIZE {
            let mut tx = engine.new_transaction().expect("start error");
            tx.insert(keys[i].clone(), keys[i].clone())
              .expect("insert error");
            tx.commit().expect("commit error");
          }
        },
        BatchSize::PerIteration,
      );
    });
  group.finish();
}

fn bench_sequential_update(c: &mut Criterion) {
  const SIZE: usize = 1_000;

  let dir = TempDir::new_in(".").expect("dir failed.");
  let keys = (0..SIZE)
    .map(|i| format!("{i:06}").as_bytes().to_vec())
    .collect::<Vec<_>>();

  pre_write_keys(dir.path(), keys.iter());

  let engine = build(dir.path())
    .group_commit_count(1)
    .build()
    .expect("bootstrap error");

  let mut group = c.benchmark_group("sequential-update");
  group
    .sample_size(10)
    .measurement_time(Duration::from_secs(30))
    .throughput(Throughput::Elements(SIZE as u64))
    .bench_function("bench", |b| {
      b.iter(|| {
        for i in 0..SIZE {
          let mut tx = engine.new_transaction().expect("start failed.");
          tx.insert(keys[i].clone(), keys[i].clone())
            .expect("update failed.");
          tx.commit().expect("commit failed.");
        }
      });
    });
  group.finish();
}

fn bench_concurrent_insert(c: &mut Criterion) {
  const SIZE: usize = 100_000;
  const THREADS_COUNT: usize = 1_000;

  let keys = (0..SIZE)
    .map(|i| format!("{i:06}").as_bytes().to_vec())
    .collect::<Vec<_>>();

  let mut group = c.benchmark_group("concurrent-insert");
  group
    .sample_size(10)
    .measurement_time(Duration::from_secs(150))
    .throughput(Throughput::Elements(SIZE as u64))
    .bench_function("bench", |b| {
      b.iter_batched_ref(
        || {
          let dir = TempDir::new_in(".").expect("dir failed.");
          let engine = Arc::new(build(dir.path()).build().expect("bootstrap error"));
          let (tx, rx) = unbounded::<(Vec<u8>, Sender<()>)>();
          let threads = (0..THREADS_COUNT)
            .map(|_| {
              let rx = rx.clone();
              let e = engine.clone();
              std::thread::spawn(move || {
                while let Ok((k, done)) = rx.recv() {
                  let mut tx = e.new_transaction().expect("start error");
                  tx.insert(k.clone(), k).expect("insert error");
                  tx.commit().expect("commit error");
                  done.send(()).unwrap();
                }
              })
            })
            .collect::<Vec<_>>();
          (dir, engine, tx, threads)
        },
        |(_, _, tx, _)| {
          let mut waiting = Vec::with_capacity(SIZE);
          for i in 0..SIZE {
            let (t, r) = unbounded();
            tx.send((keys[i].clone(), t)).unwrap();
            waiting.push(r);
          }
          waiting.into_iter().for_each(|r| r.recv().unwrap());
        },
        BatchSize::PerIteration,
      );
    });
  group.finish();
}

fn bench_concurrent_update(c: &mut Criterion) {
  const SIZE: usize = 100_000;
  const THREADS_COUNT: usize = 1_000;

  let dir = TempDir::new_in(".").expect("dir failed.");
  let keys = (0..SIZE)
    .map(|i| format!("{i:06}").as_bytes().to_vec())
    .collect::<Vec<_>>();

  pre_write_keys(dir.path(), keys.iter());
  let engine = Arc::new(build(dir.path()).build().expect("bootstrap error"));
  let (tx, rx) = unbounded::<(Vec<u8>, Sender<()>)>();
  let threads = (0..THREADS_COUNT)
    .map(|_| {
      let rx = rx.clone();
      let e = engine.clone();
      std::thread::spawn(move || {
        while let Ok((k, done)) = rx.recv() {
          let mut tx = e.new_transaction().expect("start error");
          tx.insert(k.clone(), k).expect("insert error");
          tx.commit().expect("commit error");
          done.send(()).unwrap();
        }
      })
    })
    .collect::<Vec<_>>();

  let mut group = c.benchmark_group("concurrent-update");
  group
    .sample_size(10)
    .measurement_time(Duration::from_secs(30))
    .throughput(Throughput::Elements(SIZE as u64))
    .bench_function("bench", |b| {
      b.iter(|| {
        let mut waiting = Vec::with_capacity(SIZE);
        for i in 0..SIZE {
          let (t, r) = unbounded();
          tx.send((keys[i].clone(), t)).unwrap();
          waiting.push(r);
        }
        waiting.into_iter().for_each(|r| r.recv().unwrap());
      });
    });
  group.finish();

  drop(tx);
  threads.into_iter().for_each(|t| t.join().unwrap());
}

criterion_group!(
  benches,
  bench_sequential_insert,
  bench_concurrent_insert,
  bench_sequential_update,
  bench_concurrent_update,
);
criterion_main!(benches);
