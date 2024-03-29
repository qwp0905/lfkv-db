use std::{io::Write, ops::AddAssign, path::PathBuf, time::Duration};

use chrono::{Local, SecondsFormat};
use serde_json::json;

use crate::{size, BackgroundThread, BackgroundWork};

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

pub struct LoggerConfig {
  pub path: PathBuf,
  pub interval: Duration,
  pub count: usize,
}

pub struct Logger {
  channel: BackgroundThread<String, std::io::Result<()>>,
}
impl Logger {
  pub fn new(config: LoggerConfig) -> std::io::Result<Self> {
    let mut file = std::fs::OpenOptions::new()
      .create(true)
      .read(true)
      .write(true)
      .open(config.path)?;

    let mut count = 0usize;
    let channel = BackgroundThread::new(
      "logger",
      size::mb(2),
      BackgroundWork::with_timeout(config.interval, move |msg| {
        if let Some(m) = msg {
          writeln!(&mut file, "{m}").ok();
          count.add_assign(1);
          if count.lt(&config.count) {
            return Ok(());
          }
        }

        file.sync_all()?;
        count = 0;
        Ok(())
      }),
    );

    Ok(Self { channel })
  }

  pub fn info<T>(&self, message: T)
  where
    T: ToString,
  {
    self.channel.send(fmt(Level::Info, message));
  }
}
