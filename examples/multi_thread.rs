use std::{sync::Arc, time::Duration};

fn main() -> no_db::Result<()> {
  let engine = no_db::Engine::bootstrap(no_db::EngineConfig {
    max_log_size: no_db::size::mb(16),
    max_wal_buffer_size: no_db::size::mb(2),
    checkpoint_interval: Duration::from_millis(2000),
    max_cache_size: no_db::size::mb(512),
    base_dir: "./.local",
  })?;

  let a = Arc::new(engine);

  let t = no_db::ThreadPool::<no_db::Result<()>>::new(
    1000,
    1024 * 1024,
    "sdfsdf",
    None,
  );

  let s = std::time::SystemTime::now();
  let total = Arc::new(std::sync::Mutex::new(0f64));
  let c = 1000f64;
  for i in 0..(c as usize) {
    let engine = a.clone();
    let ttt = total.clone();
    t.schedule(move || {
      let start = std::time::SystemTime::now();

      let mut cursor = engine.new_transaction()?;
      let tt = T { i };
      cursor.insert(format!("{i}"), tt)?;
      *ttt.lock().unwrap() += std::time::SystemTime::now()
        .duration_since(start)
        .unwrap()
        .as_millis() as f64;
      Ok(())
    });
  }
  drop(t);
  let end = std::time::SystemTime::now()
    .duration_since(s)
    .unwrap()
    .as_millis();
  drop(a);
  println!("{} ms sldkfjlksdjflkjslkdjflks", *total.lock().unwrap() / c);
  println!("{}", (end as f64) / 1000f64);

  // let mut cursor = engine.new_transaction()?;
  // for (i, t) in cursor.scan::<T>(format!(""))?.enumerate() {
  //   println!("{:?} {}", t, i);
  // }

  // let end = std::time::SystemTime::now()
  //   .duration_since(start)
  //   .unwrap()
  //   .as_millis();
  // println!("{}", end);

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
