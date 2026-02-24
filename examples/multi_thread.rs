use std::sync::Arc;

use lfkv_db::EngineBuilder;

fn main() {
  let engine = Arc::new(EngineBuilder::new("./.local").build().unwrap());
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
