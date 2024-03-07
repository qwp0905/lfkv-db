use std::{io::Write, path::PathBuf};

use chrono::Local;
use serde_json::json;

use crate::{size, StoppableChannel, ThreadManager};

#[allow(unused)]
enum Level {
  Info,
  Error,
  Warn,
  Debug,
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

#[allow(unused)]
pub fn debug(message: String) {
  println!("{}", fmt(Level::Debug, message))
}

fn fmt(level: Level, message: String) -> String {
  json!({
    "level": match level {
      Level::Info => "info",
      Level::Error => "error",
      Level::Warn => "warn",
      Level::Debug => "debug",
    },
    "message":message,
    "at":Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
  })
  .to_string()
}

fn format<T>(level: Level, message: T) -> String
where
  T: ToString,
{
  json!({
    "level": match level {
      Level::Info => "info",
      Level::Error => "error",
      Level::Warn => "warn",
      Level::Debug => "debug",
    },
    "message":message.to_string(),
    "at":Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
  })
  .to_string()
}

pub struct LoggerConfig {
  pub path: PathBuf,
}

pub struct Logger {
  channel: StoppableChannel<String>,
}
impl Logger {
  pub fn new(config: LoggerConfig, thread: &ThreadManager) -> std::io::Result<Self> {
    let (channel, rx) = thread.generate();
    let mut file = std::fs::OpenOptions::new()
      .create(true)
      .read(true)
      .write(true)
      .open(config.path)?;

    rx.to_new("logger", size::mb(2), move |msg| {
      writeln!(file, "{}", msg).ok();
    });

    Ok(Self { channel })
  }

  pub fn info<T>(&self, message: T)
  where
    T: ToString,
  {
    self.channel.send(format(Level::Info, message))
  }
}
