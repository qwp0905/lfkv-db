use std::io::{stderr, stdout, Write};

use crate::{json_fmt, size, BackgroundThread, BackgroundWork, Level, Logger};

pub struct StdLoggerConfig {
  pub err_level: Vec<Level>,
}

pub struct StdLogger {
  stdout: BackgroundThread<String, std::io::Result<()>>,
  stderr: BackgroundThread<String, std::io::Result<()>>,
  err_level: Vec<Level>,
}
impl StdLogger {
  pub fn open(config: StdLoggerConfig) -> Self {
    Self {
      stdout: open(Box::new(stdout()), "stdout", size::mb(2)),
      stderr: open(Box::new(stderr()), "stderr", size::mb(2)),
      err_level: config.err_level,
    }
  }

  fn send<T: ToString>(&self, level: Level, message: T) {
    self
      .err_level
      .contains(&level)
      .then(|| &self.stderr)
      .unwrap_or(&self.stdout)
      .send(json_fmt(level, message));
  }
}
impl Logger for StdLogger {
  fn info<T: ToString>(&self, message: T) {
    self.send(Level::Info, message);
  }

  fn warn<T: ToString>(&self, message: T) {
    self.send(Level::Warn, message);
  }

  fn error<T: ToString>(&self, message: T) {
    self.send(Level::Error, message);
  }

  fn debug<T: ToString>(&self, message: T) {
    self.send(Level::Debug, message);
  }
}

fn open<S: ToString>(
  mut file: Box<dyn Write + Send>,
  name: S,
  size: usize,
) -> BackgroundThread<String, std::io::Result<()>> {
  BackgroundThread::new(
    name,
    size,
    BackgroundWork::no_timeout(move |msg| writeln!(&mut file, "{msg}")),
  )
}
