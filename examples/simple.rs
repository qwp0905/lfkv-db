fn main() -> no_db::Result<()> {
  let engine = no_db::Engine::bootstrap(no_db::EngineConfig {
    max_log_size: 2048,
    max_wal_buffer_size: 100,
    checkpoint_interval: 2000,
    max_cache_size: no_db::size::mb(512),
    base_dir: "./",
  })?;
  let mut cursor = engine.new_transaction()?;
  let tt = T { i: 1 };
  cursor.insert(format!("sdfl"), tt)?;
  drop(cursor);

  let mut cursor = engine.new_transaction()?;
  let t: T = cursor.get(format!("sdfl"))?;
  println!("{:?}", t);
  drop(cursor);

  drop(engine);
  println!("engine dropppedddddd");
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
