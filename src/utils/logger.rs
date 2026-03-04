use std::{panic::RefUnwindSafe, sync::Arc};

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
impl From<LogLevel> for &str {
  fn from(value: LogLevel) -> Self {
    match value {
      LogLevel::Trace => "trace",
      LogLevel::Debug => "debug",
      LogLevel::Info => "info",
      LogLevel::Warn => "warn",
      LogLevel::Error => "error",
      LogLevel::Fatal => "fatal",
    }
  }
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
    if self.level as isize <= level as isize {
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
