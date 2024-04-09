mod file;
use chrono::{Local, SecondsFormat};
pub use file::*;
use serde_json::json;

#[derive(Debug, PartialEq)]
pub enum Level {
  Info,
  Error,
  Warn,
  Debug,
}

pub trait Logger {
  fn info<T: ToString>(&self, message: T);
  fn warn<T: ToString>(&self, message: T);
  fn error<T: ToString>(&self, message: T);
  fn debug<T: ToString>(&self, message: T);
}

pub fn json_fmt<T: ToString>(level: Level, message: T) -> String {
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
