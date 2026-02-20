use chrono::{Local, SecondsFormat};
use serde_json::json;

#[allow(unused)]
enum Level {
  Info,
  Error,
  Warn,
  Debug,
}

#[allow(unused)]
pub fn info<T: ToString>(message: T) {
  println!("{}", fmt(Level::Info, message))
}

#[allow(unused)]
pub fn warn<T: ToString>(message: T) {
  println!("{}", fmt(Level::Warn, message));
}

#[allow(unused)]
pub fn error<T: ToString>(message: T) {
  eprintln!("{}", fmt(Level::Error, message));
}

#[allow(unused)]
pub fn debug<T: ToString>(message: T) {
  println!("{}", fmt(Level::Debug, message))
}

fn fmt<T: ToString>(level: Level, message: T) -> String {
  json!({
    "level": match level {
      Level::Info => "info",
      Level::Error => "error",
      Level::Warn => "warn",
      Level::Debug => "debug",
    },
    "message": message.to_string(),
    "at": Local::now().to_rfc3339_opts(SecondsFormat::Millis, true)
  })
  .to_string()
}
