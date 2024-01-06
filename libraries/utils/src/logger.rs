use chrono::Local;
use serde_json::json;

#[allow(unused)]
enum Level {
  Info,
  Error,
  Warn,
}

#[allow(unused)]
pub fn info(message: String) {
  println!("{}", fmt(Level::Info, message))
}

#[allow(unused)]
pub fn warn(message: String) {
  println!("{}", fmt(Level::Warn, message));
}

#[allow(unused)]
pub fn error(message: String) {
  eprintln!("{}", fmt(Level::Error, message));
}

fn fmt(level: Level, message: String) -> String {
  json!({
    "level": match level {
      Level::Info => "info",
      Level::Error => "error",
      Level::Warn => "warn",
    },
    "message":message,
    "at":Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
  })
  .to_string()
}
