use std::{sync::Arc, time::Duration};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use crossbeam::channel::{unbounded, Sender};
use lfkv_db::EngineBuilder;
use rand::Rng;
use rand_distr::{Distribution, Zipf};
use tempfile::TempDir;

const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 256;
const RECORD_COUNT: usize = 100_000;
const OP_COUNT: usize = 10_000;
const THREADS: usize = 128;
const SCAN_LENGTH: usize = 100;
const ZIPF_EXPONENT: f64 = 0.99;
const DEFAULT_SAMPLE_SIZE: usize = 30;

fn make_key(i: usize) -> Vec<u8> {
  format!("{i:0>width$}", width = KEY_SIZE)
    .as_bytes()
    .to_vec()
}

fn make_value(i: usize) -> Vec<u8> {
  let mut v = format!("{i:0>width$}", width = KEY_SIZE)
    .as_bytes()
    .to_vec();
  v.resize(VALUE_SIZE, b'x');
  v
}

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

fn pre_load(dir: &std::path::Path, count: usize) {
  let engine = build(dir).build().unwrap();
  let mut tx = engine.new_transaction().unwrap();
  (0..count)
    .map(|i| (make_key(i), make_value(i)))
    .for_each(|(k, v)| tx.insert(k, v).unwrap());
  tx.commit().unwrap();
}

enum Op {
  Get(Vec<u8>),
  Insert(Vec<u8>, Vec<u8>),
  Scan(Vec<u8>, Vec<u8>),
}

fn spawn_workers(
  engine: Arc<lfkv_db::Engine>,
  count: usize,
) -> (Sender<(Op, Sender<()>)>, Vec<std::thread::JoinHandle<()>>) {
  let (tx, rx) = unbounded::<(Op, Sender<()>)>();
  let threads = (0..count)
    .map(|_| {
      let rx = rx.clone();
      let e = engine.clone();
      std::thread::spawn(move || {
        while let Ok((op, done)) = rx.recv() {
          loop {
            let mut tx = e.new_transaction().expect("start error");
            let conflict = match &op {
              Op::Get(k) => {
                tx.get(k).expect("get error");
                false
              }
              Op::Insert(k, v) => match tx.insert(k.clone(), v.clone()) {
                Ok(_) => false,
                Err(lfkv_db::Error::WriteConflict) => true,
                Err(e) => panic!("insert error: {e}"),
              },
              Op::Scan(start, end) => {
                let mut iter = tx.scan(start, end).expect("scan error");
                while let Ok(Some(_)) = iter.try_next() {}
                false
              }
            };
            if conflict {
              continue;
            }
            tx.commit().expect("commit error");
            break;
          }
          done.send(()).unwrap();
        }
      })
    })
    .collect();
  (tx, threads)
}

fn zipfian_index(rng: &mut impl Rng, zipf: &Zipf<f64>) -> usize {
  let sample: f64 = zipf.sample(rng);
  (sample as usize).saturating_sub(1).min(RECORD_COUNT - 1)
}

/// Workload A: 50% read, 50% update (write-heavy, session store)
fn bench_ycsb_a(c: &mut Criterion) {
  let dir = TempDir::new_in(".").expect("dir failed.");
  pre_load(dir.path(), RECORD_COUNT);

  let engine = Arc::new(build(dir.path()).build().expect("bootstrap error"));
  let (tx, threads) = spawn_workers(engine.clone(), THREADS);
  let zipf = Zipf::new(RECORD_COUNT as u64, ZIPF_EXPONENT).unwrap();

  let mut group = c.benchmark_group("ycsb-a");
  group
    .sample_size(DEFAULT_SAMPLE_SIZE)
    .measurement_time(Duration::from_secs(20))
    .throughput(Throughput::Elements(OP_COUNT as u64))
    .bench_function("50read-50update", |b| {
      b.iter(|| {
        let mut rng = rand::thread_rng();
        let mut waiting = Vec::with_capacity(OP_COUNT);
        for _ in 0..OP_COUNT {
          let idx = zipfian_index(&mut rng, &zipf);
          let key = make_key(idx);
          let op = if rng.gen_bool(0.50) {
            Op::Get(key)
          } else {
            Op::Insert(key, make_value(idx))
          };
          let (t, r) = unbounded();
          tx.send((op, t)).unwrap();
          waiting.push(r);
        }
        waiting.into_iter().for_each(|r| r.recv().unwrap());
      });
    });
  group.finish();

  drop(tx);
  threads.into_iter().for_each(|t| t.join().unwrap());
}

/// Workload B: 95% read, 5% update (read-heavy, typical web app)
fn bench_ycsb_b(c: &mut Criterion) {
  let dir = TempDir::new_in(".").expect("dir failed.");
  pre_load(dir.path(), RECORD_COUNT);

  let engine = Arc::new(build(dir.path()).build().expect("bootstrap error"));
  let (tx, threads) = spawn_workers(engine.clone(), THREADS);
  let zipf = Zipf::new(RECORD_COUNT as u64, ZIPF_EXPONENT).unwrap();

  let mut group = c.benchmark_group("ycsb-b");
  group
    .sample_size(DEFAULT_SAMPLE_SIZE)
    .measurement_time(Duration::from_secs(20))
    .throughput(Throughput::Elements(OP_COUNT as u64))
    .bench_function("95read-5update", |b| {
      b.iter(|| {
        let mut rng = rand::thread_rng();
        let mut waiting = Vec::with_capacity(OP_COUNT);
        for _ in 0..OP_COUNT {
          let idx = zipfian_index(&mut rng, &zipf);
          let key = make_key(idx);
          let op = if rng.gen_bool(0.95) {
            Op::Get(key)
          } else {
            Op::Insert(key, make_value(idx))
          };
          let (t, r) = unbounded();
          tx.send((op, t)).unwrap();
          waiting.push(r);
        }
        waiting.into_iter().for_each(|r| r.recv().unwrap());
      });
    });
  group.finish();

  drop(tx);
  threads.into_iter().for_each(|t| t.join().unwrap());
}

/// Workload E: 95% scan, 5% insert (range query heavy, analytics)
fn bench_ycsb_e(c: &mut Criterion) {
  let dir = TempDir::new_in(".").expect("dir failed.");
  pre_load(dir.path(), RECORD_COUNT);

  let engine = Arc::new(build(dir.path()).build().expect("bootstrap error"));
  let (tx, threads) = spawn_workers(engine.clone(), THREADS);
  let zipf = Zipf::new(RECORD_COUNT as u64, ZIPF_EXPONENT).unwrap();
  let insert_counter = std::sync::atomic::AtomicUsize::new(RECORD_COUNT);

  let mut group = c.benchmark_group("ycsb-e");
  group
    .sample_size(DEFAULT_SAMPLE_SIZE)
    .measurement_time(Duration::from_secs(20))
    .throughput(Throughput::Elements(OP_COUNT as u64))
    .bench_function("95scan-5insert", |b| {
      b.iter(|| {
        let mut rng = rand::thread_rng();
        let mut waiting = Vec::with_capacity(OP_COUNT);
        for _ in 0..OP_COUNT {
          let op = if rng.gen_bool(0.95) {
            let idx = zipfian_index(&mut rng, &zipf);
            let start = make_key(idx);
            let end = make_key((idx + SCAN_LENGTH).min(RECORD_COUNT - 1));
            Op::Scan(start, end)
          } else {
            let idx = insert_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Op::Insert(make_key(idx), make_value(idx))
          };
          let (t, r) = unbounded();
          tx.send((op, t)).unwrap();
          waiting.push(r);
        }
        waiting.into_iter().for_each(|r| r.recv().unwrap());
      });
    });
  group.finish();

  drop(tx);
  threads.into_iter().for_each(|t| t.join().unwrap());
}

criterion_group!(ycsb, bench_ycsb_a, bench_ycsb_b, bench_ycsb_e);
criterion_main!(ycsb);
