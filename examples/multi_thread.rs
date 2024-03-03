use std::{sync::Arc, time::Duration};

use lfkv_db::{size, Engine, EngineConfig};

// use std::{sync::Arc, time::Duration};
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

  let key = b"sdfsdfs".to_vec();

  let cursor = engine.new_transaction().unwrap();
  cursor.insert(key.clone(), T { i: 1 }).unwrap();
  cursor.commit().unwrap();

  let cursor = engine.new_transaction().unwrap();
  let t: T = cursor.get(key.as_ref()).unwrap();
  println!("{:?}", t);
}
// fn main() -> no_db::Result<()> {
//   let engine = no_db::Engine::bootstrap(no_db::EngineConfig {
//     max_log_size: no_db::size::mb(16),
//     max_wal_buffer_size: no_db::size::mb(2),
//     checkpoint_interval: Duration::from_secs(10),
//     max_cache_size: no_db::size::mb(512),
//     base_dir: "./.local",
//   })?;

//   let a = Arc::new(engine);

//   let t = no_db::ThreadPool::<no_db::Result<()>>::new(
//     1000,
//     1024 * 1024,
//     "sdfsdf",
//     None,
//   );

//   let s = std::time::SystemTime::now();
//   let total = Arc::new(std::sync::Mutex::new(0f64));
//   let c = 1000;
//   for i in 0..c {
//     let engine = a.clone();
//     let ttt = total.clone();
//     t.schedule(move || {
//       let start = std::time::SystemTime::now();

//       let mut cursor = engine.new_transaction()?;
//       let tt = T { i };
//       cursor.insert(format!("{:0>8}", i), tt)?;
//       *ttt.lock().unwrap() += std::time::SystemTime::now()
//         .duration_since(start)
//         .unwrap()
//         .as_millis() as f64;
//       Ok(())
//     });
//   }
//   drop(t);
//   let end = std::time::SystemTime::now()
//     .duration_since(s)
//     .unwrap()
//     .as_millis();
//   drop(a);
//   println!(
//     "{} ms sldkfjlksdjflkjslkdjflks",
//     *total.lock().unwrap() / (c as f64)
//   );
//   println!("{}", (end as f64) / 1000f64);

//   // let mut cursor = engine.new_transaction()?;
//   // for (i, t) in cursor.scan::<T>(format!(""))?.enumerate() {
//   //   println!("{:?} {}", t, i);
//   // }

//   Ok(())
// }

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
