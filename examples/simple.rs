use lfkv_db::EngineBuilder;

fn main() {
  let engine = EngineBuilder::new("./.local")
    .buffer_pool_memory_capacity(100 << 10)
    .build()
    .expect("bootstrap error");

  let mut w = engine.new_transaction().expect("write tx error");
  w.insert(b"123".to_vec(), b"456".to_vec())
    .expect("insert error");
  w.commit().expect("write commit error");

  let mut r = engine.new_transaction().expect("read tx error");
  println!("{:?}", r.get(&b"123".to_vec()).expect("find error"));
  r.commit().expect("read commit error");
}
