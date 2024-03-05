use std::{sync::Arc, time::Duration};

use lfkv_db::{size, Engine, EngineConfig};

fn main() {
  let engine = Arc::new(
    Engine::bootstrap(EngineConfig {
      base_path: "./.local",
      disk_batch_delay: Duration::from_millis(10),
      disk_batch_size: 100,
      defragmentation_interval: Duration::from_secs(30 * 60),
      undo_batch_delay: Duration::from_millis(10),
      undo_batch_size: 100,
      undo_file_size: size::mb(16),
      wal_file_size: size::mb(16),
      checkpoint_interval: Duration::from_secs(30),
      checkpoint_count: 10000,
      group_commit_delay: Duration::from_millis(10),
      group_commit_count: 100,
    })
    .unwrap(),
  );
  // let mut v = vec![];
  // for i in 0..1 {
  //   let e = engine.clone();
  //   let (tx, rx) = crossbeam::channel::unbounded();
  //   v.push(rx);
  //   std::thread::spawn(move || {
  //     let r = e.new_transaction().and_then(|t| {
  //       t.insert(format!("000{i}",).as_bytes().to_vec(), T { i })?;
  //       t.commit()?;
  //       Ok(())
  //     });
  //     tx.send(r).unwrap();
  //   });
  // }

  // for r in v {
  //   if let Err(err) = r.recv().unwrap() {
  //     println!("{:?}", err)
  //   }
  // }

  engine
    .new_transaction()
    .and_then(|c| {
      println!("{:?}", c.get::<T>(b"0000".to_vec().as_ref()).unwrap());
      c.commit()
    })
    .unwrap();
}

#[derive(Debug)]
struct T {
  i: usize,
}

impl lfkv_db::Serializable for T {
  fn deserialize(
    value: &lfkv_db::Page,
  ) -> std::prelude::v1::Result<Self, lfkv_db::Error> {
    let i = value.scanner().read_usize()?;
    Ok(T { i })
  }

  fn serialize(&self) -> std::prelude::v1::Result<lfkv_db::Page, lfkv_db::Error> {
    let mut p = lfkv_db::Page::new();
    let mut wt = p.writer();
    wt.write(&self.i.to_be_bytes())?;
    Ok(p)
  }
}
