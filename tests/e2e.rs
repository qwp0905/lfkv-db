use std::sync::{
  atomic::{AtomicBool, Ordering},
  Arc, Mutex,
};
use std::thread;
use std::time::Duration;

use lfkv_db::{Engine, EngineBuilder, Error};
use tempfile::{tempdir_in, TempDir};

fn build_engine(dir: &TempDir) -> Engine {
  EngineBuilder::new(dir.path())
    .group_commit_delay(Duration::from_millis(1))
    .group_commit_count(10)
    .build()
    .expect("engine bootstrap failed")
}

// ============================================================
// 1. Basic CRUD
// ============================================================
#[test]
fn test_basic_crud() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let mut tx = engine.new_transaction().unwrap();
  tx.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();

  // read within same tx
  let val = tx.get(&b"key1".to_vec()).unwrap();
  assert_eq!(val, Some(b"value1".to_vec()));

  // remove
  tx.remove(&b"key1".to_vec()).unwrap();
  let val = tx.get(&b"key1".to_vec()).unwrap();
  assert_eq!(val, None);

  tx.commit().unwrap();
}

// ============================================================
// 2. Commit Visibility
// ============================================================
#[test]
fn test_commit_visibility() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let mut tx1 = engine.new_transaction().unwrap();
  tx1.insert(b"hello".to_vec(), b"world".to_vec()).unwrap();
  tx1.commit().unwrap();

  let tx2 = engine.new_transaction().unwrap();
  let val = tx2.get(&b"hello".to_vec()).unwrap();
  assert_eq!(val, Some(b"world".to_vec()));
}

// ============================================================
// 3. Abort Invisibility
// ============================================================
#[test]
fn test_abort_invisibility() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let mut tx1 = engine.new_transaction().unwrap();
  tx1.insert(b"ghost".to_vec(), b"data".to_vec()).unwrap();
  tx1.abort().unwrap();

  let tx2 = engine.new_transaction().unwrap();
  let val = tx2.get(&b"ghost".to_vec()).unwrap();
  assert_eq!(val, None);
}

// ============================================================
// 4. Drop Abort (implicit abort on drop)
// ============================================================
#[test]
fn test_drop_abort() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  {
    let mut tx1 = engine.new_transaction().unwrap();
    tx1.insert(b"vanish".to_vec(), b"poof".to_vec()).unwrap();
    // no commit, no abort — just drop
  }

  let tx2 = engine.new_transaction().unwrap();
  let val = tx2.get(&b"vanish".to_vec()).unwrap();
  assert_eq!(val, None);
}

// ============================================================
// 5. Write Conflict
// ============================================================
#[test]
fn test_write_conflict() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let mut tx1 = engine.new_transaction().unwrap();
  tx1.insert(b"contested".to_vec(), b"v1".to_vec()).unwrap();
  // tx1 NOT committed — still active

  let mut tx2 = engine.new_transaction().unwrap();
  let result = tx2.insert(b"contested".to_vec(), b"v2".to_vec());
  assert!(result.is_err());
  if let Err(Error::WriteConflict) = result {
    // expected
  } else {
    panic!("expected WriteConflict");
  }

  tx1.commit().unwrap();
}

// ============================================================
// 6. TransactionClosed
// ============================================================
#[test]
fn test_transaction_closed_after_commit() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let mut tx = engine.new_transaction().unwrap();
  tx.insert(b"k".to_vec(), b"v".to_vec()).unwrap();
  tx.commit().unwrap();

  assert!(matches!(
    tx.insert(b"k2".to_vec(), b"v2".to_vec()),
    Err(Error::TransactionClosed)
  ));
  assert!(matches!(
    tx.get(&b"k".to_vec()),
    Err(Error::TransactionClosed)
  ));
  assert!(matches!(
    tx.remove(&b"k".to_vec()),
    Err(Error::TransactionClosed)
  ));
  assert!(matches!(tx.scan_all(), Err(Error::TransactionClosed)));
  assert!(matches!(tx.commit(), Err(Error::TransactionClosed)));
  assert!(matches!(tx.abort(), Err(Error::TransactionClosed)));
}

#[test]
fn test_transaction_closed_after_abort() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let mut tx = engine.new_transaction().unwrap();
  tx.insert(b"k".to_vec(), b"v".to_vec()).unwrap();
  tx.abort().unwrap();

  assert!(matches!(
    tx.insert(b"k2".to_vec(), b"v2".to_vec()),
    Err(Error::TransactionClosed)
  ));
  assert!(matches!(
    tx.get(&b"k".to_vec()),
    Err(Error::TransactionClosed)
  ));
}

// ============================================================
// 7. Scan
// ============================================================
#[test]
fn test_scan_range() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let mut tx = engine.new_transaction().unwrap();
  for i in 0u8..10 {
    tx.insert(vec![i], vec![i * 10]).unwrap();
  }
  tx.commit().unwrap();

  let tx2 = engine.new_transaction().unwrap();

  // scan [3, 7)
  let mut iter = tx2.scan(&vec![3], &vec![7]).unwrap();
  let mut results = vec![];
  while let Some((k, v)) = iter.try_next().unwrap() {
    results.push((k, v));
  }
  assert_eq!(results.len(), 4); // keys 3,4,5,6
  assert_eq!(results[0], (vec![3], vec![30]));
  assert_eq!(results[3], (vec![6], vec![60]));
}

#[test]
fn test_scan_all() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let mut tx = engine.new_transaction().unwrap();
  for i in 0u8..5 {
    tx.insert(vec![i], vec![i]).unwrap();
  }
  tx.commit().unwrap();

  let tx2 = engine.new_transaction().unwrap();
  let mut iter = tx2.scan_all().unwrap();
  let mut results = vec![];
  while let Some((k, v)) = iter.try_next().unwrap() {
    results.push((k, v));
  }
  assert_eq!(results.len(), 5);
  for i in 0u8..5 {
    assert_eq!(results[i as usize], (vec![i], vec![i]));
  }
}

// ============================================================
// 8. Overwrite
// ============================================================
#[test]
fn test_overwrite() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let mut tx1 = engine.new_transaction().unwrap();
  tx1.insert(b"key".to_vec(), b"v1".to_vec()).unwrap();
  tx1.commit().unwrap();

  let mut tx2 = engine.new_transaction().unwrap();
  tx2.insert(b"key".to_vec(), b"v2".to_vec()).unwrap();
  tx2.commit().unwrap();

  let tx3 = engine.new_transaction().unwrap();
  let val = tx3.get(&b"key".to_vec()).unwrap();
  assert_eq!(val, Some(b"v2".to_vec()));
}

// ============================================================
// 9. Crash Recovery
// ============================================================
#[test]
fn test_crash_recovery() {
  let dir = tempdir_in(".").unwrap();
  let committed_keys: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(vec![]));
  let stop = Arc::new(AtomicBool::new(false));

  // Phase 1: concurrent writes, then drop engine mid-flight
  {
    let engine = Arc::new(build_engine(&dir));
    let mut handles = vec![];

    for t in 0..4u8 {
      let engine = engine.clone();
      let committed = committed_keys.clone();
      let stop = stop.clone();

      handles.push(thread::spawn(move || {
        for i in 0..100u8 {
          if stop.load(Ordering::Relaxed) {
            break;
          }
          let key = vec![t, i];
          let value = vec![t, i, 0xFF];
          let mut tx = match engine.new_transaction() {
            Ok(tx) => tx,
            Err(_) => break,
          };
          if tx.insert(key.clone(), value).is_err() {
            continue;
          }
          if tx.commit().is_ok() {
            committed.lock().unwrap().push(key);
          }
        }
      }));
    }

    // let workers run briefly then kill engine
    thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
      let _ = h.join();
    }
    // engine dropped here
  }

  // Phase 2: restart and verify
  let engine = build_engine(&dir);
  let tx = engine.new_transaction().unwrap();
  let keys = committed_keys.lock().unwrap();

  assert!(!keys.is_empty(), "should have committed at least some keys");

  for key in keys.iter() {
    let val = tx.get(key).unwrap();
    assert!(
      val.is_some(),
      "committed key {:?} missing after recovery",
      key
    );
    let v = val.unwrap();
    assert_eq!(v[0], key[0]);
    assert_eq!(v[1], key[1]);
    assert_eq!(v[2], 0xFF);
  }
}

// ============================================================
// 10. Snapshot Isolation
// ============================================================
#[test]
fn test_snapshot_isolation() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  // tx1 and tx2 both start
  let mut tx1 = engine.new_transaction().unwrap();
  let tx2 = engine.new_transaction().unwrap();

  // tx1 inserts and commits AFTER tx2 started
  tx1
    .insert(b"after".to_vec(), b"should-not-see".to_vec())
    .unwrap();
  tx1.commit().unwrap();

  // tx2 should NOT see tx1's write (snapshot isolation)
  let val = tx2.get(&b"after".to_vec()).unwrap();
  assert_eq!(val, None, "tx2 should not see tx1's post-start commit");
  drop(tx2);

  // tx3 starts AFTER tx1 committed — should see it
  let tx3 = engine.new_transaction().unwrap();
  let val = tx3.get(&b"after".to_vec()).unwrap();
  assert_eq!(val, Some(b"should-not-see".to_vec()));
}

// ============================================================
// 11. Entry Split (many versions on single key)
// ============================================================
#[test]
fn test_entry_split() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let key = b"hot-key".to_vec();
  // 100-byte value * 50 txs → well over 4KB page, forces split
  let iterations = 50;

  for i in 0..iterations {
    let mut tx = engine.new_transaction().unwrap();
    let value = vec![i as u8; 100];
    tx.insert(key.clone(), value).unwrap();
    tx.commit().unwrap();
  }

  // verify latest value is readable
  let tx = engine.new_transaction().unwrap();
  let val = tx.get(&key).unwrap();
  assert!(
    val.is_some(),
    "hot key should be readable after many overwrites"
  );
  let v = val.unwrap();
  assert_eq!(v.len(), 100);
  assert_eq!(v[0], (iterations - 1) as u8);
}

// ============================================================
// 12. Large-scale Insert + Scan + Recovery (B-Tree node splits)
// ============================================================
#[test]
fn test_btree_node_split_and_recovery() {
  let dir = tempdir_in(".").unwrap();
  let key_count: usize = 5000;

  // Phase 1: worker pool (100 threads) concurrently insert → forces leaf + internal node splits
  {
    let engine = Arc::new(build_engine(&dir));
    let thread_count = 100;
    let (task_tx, task_rx) =
      crossbeam::channel::unbounded::<(usize, crossbeam::channel::Sender<()>)>();

    let mut workers = Vec::new();
    for _ in 0..thread_count {
      let rx = task_rx.clone();
      let e = engine.clone();
      workers.push(thread::spawn(move || {
        while let Ok((i, done)) = rx.recv() {
          let mut cursor = e.new_transaction().unwrap();
          let key = format!("key-{:06}", i).into_bytes();
          let value = format!("val-{:06}", i).into_bytes();
          cursor.insert(key, value).unwrap();
          cursor.commit().unwrap();
          done.send(()).unwrap();
        }
      }));
    }

    let mut completions = Vec::new();
    for i in 0..key_count {
      let (done_tx, done_rx) = crossbeam::channel::unbounded();
      task_tx.send((i, done_tx)).unwrap();
      completions.push(done_rx);
    }
    completions.into_iter().for_each(|r| r.recv().unwrap());
    drop(task_tx);
    for w in workers {
      w.join().expect("worker panicked");
    }

    // scan all and verify order + completeness
    let tx = engine.new_transaction().unwrap();
    let mut iter = tx.scan_all().unwrap();
    let mut count = 0;
    let mut prev_key: Option<Vec<u8>> = None;
    while let Some((k, v)) = iter.try_next().unwrap() {
      // keys should be in sorted order
      if let Some(ref pk) = prev_key {
        assert!(k > *pk, "keys not sorted: {:?} >= {:?}", pk, k);
      }
      prev_key = Some(k.clone());

      // value matches key
      let expected_val = String::from_utf8_lossy(&k)
        .replacen("key", "val", 1)
        .into_bytes();
      assert_eq!(
        v,
        expected_val,
        "value mismatch for key {:?}",
        String::from_utf8_lossy(&k)
      );
      count += 1;
    }
    assert_eq!(
      count, key_count,
      "scan should return all {} keys",
      key_count
    );

    // point-read spot checks
    for i in [0, 1, 500, 2500, 4999] {
      let key = format!("key-{:06}", i).into_bytes();
      let val = tx.get(&key).unwrap();
      assert!(
        val.is_some(),
        "key {:?} missing",
        String::from_utf8_lossy(&key)
      );
    }
    // engine dropped here
  }

  // Phase 2: restart engine and verify persistence
  {
    let engine = build_engine(&dir);
    let tx = engine.new_transaction().unwrap();

    let mut iter = tx.scan_all().unwrap();
    let mut count = 0;
    let mut prev_key: Option<Vec<u8>> = None;
    while let Some((k, v)) = iter.try_next().unwrap() {
      if let Some(ref pk) = prev_key {
        assert!(k > *pk, "keys not sorted after recovery");
      }
      prev_key = Some(k.clone());
      let expected_val = String::from_utf8_lossy(&k)
        .replacen("key", "val", 1)
        .into_bytes();
      assert_eq!(
        v,
        expected_val,
        "post-recovery value mismatch for {:?}",
        String::from_utf8_lossy(&k)
      );
      count += 1;
    }
    assert_eq!(
      count, key_count,
      "all {} keys should survive recovery",
      key_count
    );
  }
}

// ============================================================
// 13. Crash Recovery via process::exit
// ============================================================

// Child process: concurrent writes then process::exit (no drop, no flush)
#[test]
#[ignore]
fn crash_writer() {
  let dir = std::env::var("CRASH_DIR").expect("CRASH_DIR not set");
  let engine = EngineBuilder::new(std::path::Path::new(&dir))
    .group_commit_delay(Duration::from_millis(1))
    .group_commit_count(10)
    .build()
    .expect("engine bootstrap failed");

  let key_count: usize = 5000;
  let thread_count = 100;
  let (task_tx, task_rx) =
    crossbeam::channel::unbounded::<(usize, crossbeam::channel::Sender<()>)>();
  let engine = Arc::new(engine);

  for _ in 0..thread_count {
    let rx = task_rx.clone();
    let e = engine.clone();
    thread::spawn(move || {
      while let Ok((i, done)) = rx.recv() {
        let mut cursor = e.new_transaction().unwrap();
        let key = format!("key-{:06}", i).into_bytes();
        let value = format!("val-{:06}", i).into_bytes();
        cursor.insert(key, value).unwrap();
        cursor.commit().unwrap();
        let _ = std::io::Write::write_all(
          &mut std::io::stdout().lock(),
          format!("{}\n", i).as_bytes(),
        );
        let _ = done.send(());
      }
    });
  }

  let mut completions = Vec::new();
  for i in 0..key_count {
    let (done_tx, done_rx) = crossbeam::channel::unbounded();
    task_tx.send((i, done_tx)).unwrap();
    completions.push(done_rx);
  }

  let mut c = 0;
  for done in completions {
    let _ = done.recv();
    c += 1;
    if c == key_count / 2 {
      std::process::exit(0);
    }
  }
}

// Parent process: spawn crash_writer, collect committed keys, verify recovery
#[test]
fn test_process_crash_recovery() {
  use std::io::BufRead;
  use std::process::{Command, Stdio};

  let dir = tempdir_in(".").unwrap();

  // Phase 1: spawn child that writes concurrently then crashes
  let mut child = Command::new(std::env::current_exe().unwrap())
    .arg("--ignored")
    .arg("--exact")
    .arg("crash_writer")
    .arg("--nocapture")
    .env("CRASH_DIR", dir.path())
    .stdout(Stdio::piped())
    .stderr(Stdio::null())
    .spawn()
    .expect("failed to spawn crash_writer");

  let stdout = child.stdout.take().unwrap();
  let reader = std::io::BufReader::new(stdout);

  let mut committed = std::collections::HashSet::new();
  for line in reader.lines() {
    match line {
      Ok(s) => {
        if let Ok(i) = s.trim().parse::<usize>() {
          committed.insert(i);
        }
      }
      Err(_) => break,
    }
  }

  let _ = child.wait();

  assert!(
    !committed.is_empty(),
    "child should have committed at least some keys"
  );

  // Phase 2: reopen engine and verify all committed keys survived
  {
    let engine = build_engine(&dir);
    let tx = engine.new_transaction().unwrap();
    for i in &committed {
      let key = format!("key-{:06}", i).into_bytes();
      let expected = format!("val-{:06}", i).into_bytes();
      let val = tx.get(&key).unwrap();
      assert_eq!(
        val,
        Some(expected),
        "committed key {} missing after crash recovery",
        i
      );
    }
  }

  eprintln!(
    "crash recovery: {} keys committed, all verified",
    committed.len()
  );
}
