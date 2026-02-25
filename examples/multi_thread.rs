use std::sync::Arc;

use lfkv_db::EngineBuilder;

fn main() {
  let engine = Arc::new(EngineBuilder::new("./.local").build().unwrap());
  let mut v = vec![];
  for i in 0..100 {
    let e = engine.clone();
    let (tx, rx) = crossbeam::channel::unbounded();
    v.push(rx);
    std::thread::spawn(move || {
      let mut r = e.new_transaction().expect("start error");
      let vec = format!("123{}", i).as_bytes().to_vec();
      r.insert(vec![i], vec).expect("insert error");
      r.commit().expect("commit error");
      tx.send(()).unwrap();
      println!("{i} insert ok")
    });
  }
  v.into_iter().for_each(|r| r.recv().unwrap());

  let mut t = engine.new_transaction().expect("scan start error");
  let mut iter = t.scan_all().expect("scan all error");
  while let Ok(Some((k, v))) = iter.try_next() {
    println!("{:?} {:?}", k, String::from_utf8_lossy(&v))
  }

  t.commit().expect("scan commit error");
}
