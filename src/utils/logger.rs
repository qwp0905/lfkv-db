use std::{panic::RefUnwindSafe, sync::Arc};

use chrono::{Local, SecondsFormat};
use serde_json::json;

#[allow(unused)]
#[derive(Clone, Copy)]
pub enum LogLevel {
  Trace = 0,
  Debug = 1,
  Info = 2,
  Warn = 3,
  Error = 4,
  Fatal = 5,
}

#[allow(unused)]
pub fn info<T: ToString>(message: T) {
  println!("{}", fmt(LogLevel::Info, message))
}

#[allow(unused)]
pub fn warn<T: ToString>(message: T) {
  println!("{}", fmt(LogLevel::Warn, message));
}

#[allow(unused)]
pub fn error<T: ToString>(message: T) {
  eprintln!("{}", fmt(LogLevel::Error, message));
}

#[allow(unused)]
pub fn debug<T: ToString>(message: T) {
  println!("{}", fmt(LogLevel::Debug, message))
}

fn fmt<T: ToString>(level: LogLevel, message: T) -> String {
  json!({
    "level": match level {
        LogLevel::Info=>"info",
        LogLevel::Error=>"error",
        LogLevel::Warn=>"warn",
        LogLevel::Debug=>"debug",
        LogLevel::Trace => "trace",
        LogLevel::Fatal => "fatal",
    },
    "message": message.to_string(),
    "at": Local::now().to_rfc3339_opts(SecondsFormat::Millis, true)
  })
  .to_string()
}

pub trait Logger: Send + Sync + RefUnwindSafe {
  fn log(&self, level: LogLevel, msg: &[u8]);
}
pub struct LogFilter {
  level: LogLevel,
  logger: Arc<dyn Logger>,
}
impl LogFilter {
  pub fn new(level: LogLevel, logger: Arc<dyn Logger>) -> Self {
    Self { level, logger }
  }

  fn log(&self, level: LogLevel, msg: &[u8]) {
    if self.level as isize >= level as isize {
      self.logger.log(level, msg)
    }
  }
  pub fn info<T: AsRef<[u8]>>(&self, msg: T) {
    self.log(LogLevel::Info, msg.as_ref())
  }
  pub fn warn<T: AsRef<[u8]>>(&self, msg: T) {
    self.log(LogLevel::Warn, msg.as_ref())
  }
  pub fn error<T: AsRef<[u8]>>(&self, msg: T) {
    self.log(LogLevel::Error, msg.as_ref())
  }
  pub fn fatal<T: AsRef<[u8]>>(&self, msg: T) {
    self.log(LogLevel::Fatal, msg.as_ref())
  }
  pub fn debug<T: AsRef<[u8]>>(&self, msg: T) {
    self.log(LogLevel::Debug, msg.as_ref())
  }
  pub fn trace<T: AsRef<[u8]>>(&self, msg: T) {
    self.log(LogLevel::Trace, msg.as_ref())
  }
}
impl Clone for LogFilter {
  fn clone(&self) -> Self {
    Self {
      level: self.level,
      logger: self.logger.clone(),
    }
  }
}

pub struct NoneLogger;
impl Logger for NoneLogger {
  #[allow(unused)]
  fn log(&self, level: LogLevel, msg: &[u8]) {}
}
