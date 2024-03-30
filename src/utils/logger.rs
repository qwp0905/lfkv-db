use std::{
  io::{stderr, stdout, Write},
  ops::{AddAssign, Mul},
  path::PathBuf,
  time::Duration,
};

use chrono::{Local, SecondsFormat};
use serde_json::json;

use crate::{size, BackgroundThread, BackgroundWork, DrainAll};

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

pub trait Logger {
  fn info<T: ToString>(&self, message: T);
  fn warn<T: ToString>(&self, message: T);
  fn error<T: ToString>(&self, message: T);
  fn debug<T: ToString>(&self, message: T);
}

pub struct FileLoggerConfig {
  pub path: PathBuf,
  pub interval: Duration,
  pub count: usize,
}

pub struct FileLogger {
  channel: BackgroundThread<String, std::io::Result<()>>,
}
impl FileLogger {
  pub fn new(config: FileLoggerConfig) -> std::io::Result<Self> {
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
}
impl Logger for FileLogger {
  fn info<T: ToString>(&self, message: T) {
    self.channel.send(fmt(Level::Info, message));
  }

  fn warn<T: ToString>(&self, message: T) {
    self.channel.send(fmt(Level::Warn, message));
  }

  fn error<T: ToString>(&self, message: T) {
    self.channel.send(fmt(Level::Error, message));
  }

  fn debug<T: ToString>(&self, message: T) {
    self.channel.send(fmt(Level::Debug, message));
  }
}

pub struct StandardLoggerConfig {
  pub interval: Duration,
  pub size: usize,
}

pub struct StandardLogger {
  stdout: BackgroundThread<String>,
  stderr: BackgroundThread<String>,
}
impl StandardLogger {
  pub fn new(config: StandardLoggerConfig) -> Self {
    let max_size = config.size;
    let out = stdout();
    let err = stderr();
    let mut out_buffered = String::new();
    let mut err_buffered = String::new();
    Self {
      stdout: BackgroundThread::new(
        "logger stdout",
        max_size.mul(2),
        BackgroundWork::with_timeout(
          config.interval,
          move |maybe_msg: Option<String>| {
            if let Some(msg) = maybe_msg {
              writeln!(unsafe { out_buffered.as_mut_vec() }, "{msg}").ok();
              if out_buffered.len().lt(&max_size) {
                return;
              }
            }
            writeln!(&mut out.lock(), "{}", out_buffered.drain_all()).ok();
          },
        ),
      ),
      stderr: BackgroundThread::new(
        "logger stderr",
        max_size.mul(2),
        BackgroundWork::with_timeout(config.interval, move |maybe_msg| {
          if let Some(msg) = maybe_msg {
            writeln!(unsafe { err_buffered.as_mut_vec() }, "{msg}").ok();
            if err_buffered.len().lt(&max_size) {
              return;
            }
          }
          writeln!(&mut err.lock(), "{}", err_buffered.drain_all()).ok();
        }),
      ),
    }
  }
}
impl Logger for StandardLogger {
  fn info<T: ToString>(&self, message: T) {
    self.stdout.send(fmt(Level::Info, message));
  }

  fn warn<T: ToString>(&self, message: T) {
    self.stdout.send(fmt(Level::Warn, message));
  }

  fn error<T: ToString>(&self, message: T) {
    self.stderr.send(fmt(Level::Error, message));
  }

  fn debug<T: ToString>(&self, message: T) {
    self.stdout.send(fmt(Level::Debug, message));
  }
}
