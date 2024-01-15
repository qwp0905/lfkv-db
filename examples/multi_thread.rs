use std::sync::Arc;

fn main() -> no_db::Result<()> {
  let engine = no_db::Engine::bootstrap(no_db::EngineConfig {
    max_log_size: 2048,
    max_wal_buffer_size: 100,
    checkpoint_interval: 2000,
    max_cache_size: no_db::size::mb(512),
    base_dir: "./.local",
  })?;

  let a = Arc::new(engine);
  // println!("engine created");

  // let t = no_db::ThreadPool::<no_db::Result<()>>::new(
  //   1000,
  //   1024 * 1024,
  //   "sdfsdf",
  //   None,
  // );

  // let start = std::time::SystemTime::now();
  // for i in 0..100 {
  //   let engine = a.clone();
  //   t.schedule(move || {
  //     let mut cursor = engine.new_transaction()?;
  //     let tt = T { i };
  //     cursor.insert(format!("{i}"), tt)?;
  //     // drop(cursor);
  //     // let mut cursor = engine.new_transaction()?;
  //     // let t: T = cursor.get(format!("{i}"))?;
  //     // println!("{i} {:?}", t);
  //     Ok(())
  //   });
  // }
  // drop(t);
  // let end = std::time::SystemTime::now()
  //   .duration_since(start)
  //   .unwrap()
  //   .as_millis();
  // println!("{}", end);

  let mut cursor = a.new_transaction()?;
  for t in cursor.scan::<T>(format!(""))? {
    println!("{:?}", t);
  }
  // let t: T = cursor.get(format!("2"))?;
  // println!("{:?}", t);

  Ok(())
}

#[derive(Debug)]
struct T {
  i: usize,
}

impl no_db::Serializable for T {
  fn deserialize(
    value: &no_db::Page,
  ) -> std::prelude::v1::Result<Self, no_db::Error> {
    let i = value.scanner().read_usize()?;
    Ok(T { i })
  }

  fn serialize(&self) -> std::prelude::v1::Result<no_db::Page, no_db::Error> {
    let mut p = no_db::Page::new();
    let mut wt = p.writer();
    wt.write(&self.i.to_be_bytes())?;
    return Ok(p);
  }
}
