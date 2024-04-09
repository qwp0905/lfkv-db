use std::{io::Write, ops::AddAssign, path::Path, time::Duration};

use crate::{
  json_fmt, size, BackgroundThread, BackgroundWork, Error, Level, Logger, Result,
};

pub struct FileLoggerConfig<T: AsRef<Path>> {
  pub stdout: T,
  pub stderr: T,
  pub interval: Duration,
  pub count: usize,
  pub err_level: Vec<Level>,
}
pub struct FileLogger {
  stdout: BackgroundThread<String, std::io::Result<()>>,
  stderr: BackgroundThread<String, std::io::Result<()>>,
  err_level: Vec<Level>,
}
impl FileLogger {
  pub fn open<T: AsRef<Path>>(config: FileLoggerConfig<T>) -> Result<Self> {
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
