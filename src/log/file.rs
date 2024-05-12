use std::{
  io::Write,
  ops::AddAssign,
  path::{Path, PathBuf},
  time::Duration,
};

use crate::{
  json_fmt, size, BackgroundThread, BackgroundWork, Error, Level, Logger, Result,
};

pub struct FileLoggerConfig {
  pub stdout: PathBuf,
  pub stderr: PathBuf,
  pub interval: Duration,
  pub count: usize,
  pub err_level: Vec<Level>,
}
impl FileLoggerConfig {
  pub fn new() -> Self {
    Self {
      stdout: PathBuf::from("out.log"),
      stderr: PathBuf::from("error.log"),
      interval: Duration::from_millis(100),
      count: 100,
      err_level: vec![Level::Error],
    }
  }

  pub fn stdout<T: ToString>(mut self, path: T) -> Self {
    self.stdout = PathBuf::from(path.to_string());
    self
  }

  pub fn stderr<T: ToString>(mut self, path: T) -> Self {
    self.stderr = PathBuf::from(path.to_string());
    self
  }

  pub fn interval(mut self, interval: Duration) -> Self {
    self.interval = interval;
    self
  }

  pub fn err_level(mut self, err_level: Vec<Level>) -> Self {
    self.err_level = err_level;
    self
  }

  pub fn build(self) -> Result<FileLogger> {
    FileLogger::open(self)
  }
}

pub struct FileLogger {
  stdout: BackgroundThread<String, std::io::Result<()>>,
  stderr: BackgroundThread<String, std::io::Result<()>>,
  err_level: Vec<Level>,
}
impl FileLogger {
  fn open(config: FileLoggerConfig) -> Result<Self> {
    let stdout = open_logger(
      "stdout",
      config.stdout,
      size::mb(2),
      config.interval,
      config.count,
    )
    .map_err(Error::IO)?;
    let stderr = open_logger(
      "stderr",
      config.stderr,
      size::mb(2),
      config.interval,
      config.count,
    )
    .map_err(Error::IO)?;

    Ok(Self {
      stdout,
      stderr,
      err_level: config.err_level,
    })
  }
}
impl Logger for FileLogger {
  fn info<T: ToString>(&self, message: T) {
    self
      .err_level
      .contains(&Level::Info)
      .then(|| &self.stderr)
      .unwrap_or(&self.stdout)
      .send(json_fmt(Level::Info, message));
  }

  fn warn<T: ToString>(&self, message: T) {
    self
      .err_level
      .contains(&Level::Warn)
      .then(|| &self.stderr)
      .unwrap_or(&self.stdout)
      .send(json_fmt(Level::Warn, message));
  }

  fn error<T: ToString>(&self, message: T) {
    self
      .err_level
      .contains(&Level::Error)
      .then(|| &self.stderr)
      .unwrap_or(&self.stdout)
      .send(json_fmt(Level::Error, message));
  }

  fn debug<T: ToString>(&self, message: T) {
    self
      .err_level
      .contains(&Level::Debug)
      .then(|| &self.stderr)
      .unwrap_or(&self.stdout)
      .send(json_fmt(Level::Debug, message));
  }
}

fn open_logger<T: AsRef<Path>, S: ToString>(
  name: S,
  path: T,
  size: usize,
  interval: Duration,
  max_count: usize,
) -> std::io::Result<BackgroundThread<String, std::io::Result<()>>> {
  let mut file = std::fs::OpenOptions::new()
    .create(true)
    .read(true)
    .write(true)
    .append(true)
    .open(path)?;
  let mut count = 0usize;
  Ok(BackgroundThread::new(
    name,
    size,
    BackgroundWork::with_timeout(interval, move |msg| {
      if let Some(m) = msg {
        writeln!(&mut file, "{m}")?;
        count.add_assign(1);
        if count.lt(&max_count) {
          return Ok(());
        }
      }

      file.sync_all()?;
      count = 0;
      Ok(())
    }),
  ))
}
