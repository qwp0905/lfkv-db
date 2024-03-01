use std::{
  fs::{File, Metadata, OpenOptions},
  io::{Read, Seek, SeekFrom, Write},
  ops::Mul,
  path::PathBuf,
  time::Duration,
};

use crossbeam::channel::Sender;

use crate::{
  ContextReceiver, Error, Page, Result, Serializable, StoppableChannel, UnwrappedSender,
};

enum Command<const N: usize> {
  Read(usize),
  Write(usize, Page<N>),
  Flush,
  Metadata,
}
impl<const N: usize> Command<N> {
  fn exec(
    &self,
    file: &mut File,
  ) -> std::io::Result<(Option<Page<N>>, Option<Metadata>)> {
    match self {
      Command::Read(index) => {
        file.seek(SeekFrom::Start(get_offset(*index, N)))?;
        let mut page = Page::new_empty();
        file.read_exact(page.as_mut())?;
        Ok((Some(page), None))
      }
      Command::Write(index, page) => {
        file.seek(SeekFrom::Start(get_offset(*index, N)))?;
        file.write_all(page.as_ref())?;
        Ok((None, None))
      }
      Command::Flush => file.sync_all().map(|_| (None, None)),
      Command::Metadata => file.metadata().map(|m| (None, Some(m))),
    }
  }
}

pub struct FinderConfig {
  pub path: PathBuf,
  pub batch_delay: Duration,
  pub batch_size: usize,
}

pub struct Finder<const N: usize> {
  io_c: StoppableChannel<Command<N>, Result<(Option<Page<N>>, Option<Metadata>)>>,
  batch_c: StoppableChannel<(usize, Page<N>), Result>,
  config: FinderConfig,
}
impl<const N: usize> Finder<N> {
  pub fn open(config: FinderConfig) -> Result<Self> {
    let (io_c, io_rx) = StoppableChannel::new();
    let (batch_c, batch_rx) = StoppableChannel::new();
    let finder = Self {
      io_c,
      config,
      batch_c,
    };
    finder.start_batch(batch_rx).start_io(io_rx)
  }

  fn start_io(
    self,
    rx: ContextReceiver<Command<N>, Result<(Option<Page<N>>, Option<Metadata>)>>,
  ) -> Result<Self> {
    let mut file = OpenOptions::new()
      .create(true)
      .read(true)
      .write(true)
      .open(&self.config.path)
      .map_err(Error::IO)?;
    let name = format!("{} finder io", self.config.path.to_string_lossy());
    rx.to_done(&name, N.mul(1000), move |cmd: Command<N>| {
      cmd.exec(&mut file).map_err(Error::IO)
    });
    Ok(self)
  }

  fn start_batch(self, rx: ContextReceiver<(usize, Page<N>), Result>) -> Self {
    let delay = self.config.batch_delay;
    let count = self.config.batch_size;
    let c = self.io_c.clone();
    rx.with_timer(
      "finder batch",
      N.mul(2).mul(count),
      delay,
      move |v: &mut Vec<Sender<Result>>,
            o: Option<((usize, Page<N>), Sender<Result>)>| {
        if let Some(((index, page), done)) = o {
          if let Err(err) = c.send_await(Command::Write(index, page)) {
            done.must_send(Err(err));
            return false;
          };

          v.push(done);
          if v.len().lt(&count) {
            return false;
          }
        }

        if let Err(_) = c.send_await(Command::Flush) {
          return false;
        }

        v.drain(..).for_each(|done| done.must_send(Ok(())));
        true
      },
    );
    self
  }

  pub fn read(&self, index: usize) -> Result<Page<N>> {
    let r = self.io_c.send_await(Command::Read(index))?;
    Ok(r.0.unwrap())
  }

  pub fn write(&self, index: usize, page: Page<N>) -> Result {
    self.io_c.send_await(Command::Write(index, page))?;
    Ok(())
  }

  pub fn fsync(&self) -> Result {
    self.io_c.send_await(Command::Flush)?;
    Ok(())
  }

  pub fn batch_write(&self, index: usize, page: Page<N>) -> Result {
    self.batch_c.send_await((index, page))
  }

  pub fn len(&self) -> Result<usize> {
    let r = self.io_c.send_await(Command::Metadata)?;
    Ok((r.1.unwrap().len() as usize).div_ceil(N))
  }
}

impl<const N: usize> Finder<N> {
  pub fn read_to<T>(&self, index: usize) -> Result<T>
  where
    T: Serializable<Error, N>,
  {
    let page = self.read(index)?;
    page.deserialize()
  }

  pub fn write_from<T>(&self, index: usize, v: &T) -> Result
  where
    T: Serializable<Error, N>,
  {
    let page = v.serialize()?;
    self.write(index, page)
  }

  pub fn batch_write_from<T>(&self, index: usize, v: &T) -> Result
  where
    T: Serializable<Error, N>,
  {
    let page = v.serialize()?;
    self.batch_write(index, page)
  }
}
impl<const N: usize> Drop for Finder<N> {
  fn drop(&mut self) {
    self.batch_c.terminate();
    self.io_c.terminate();
  }
}

fn get_offset(index: usize, n: usize) -> u64 {
  (index.mul(n)) as u64
}
