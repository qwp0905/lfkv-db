use std::{
  fs::{File, OpenOptions},
  io::{Read, Seek, SeekFrom, Write},
  path::PathBuf,
  time::Duration,
};

use crossbeam::channel::Sender;

use crate::{
  ContextReceiver, Error, Page, Result, Serializable, StoppableChannel, Timer,
  UnwrappedSender,
};

enum Command<const N: usize> {
  Read(usize),
  Write(usize, Page<N>),
  Flush,
}
impl<const N: usize> Command<N> {
  fn exec(&self, file: &mut File) -> std::io::Result<Option<Page<N>>> {
    match self {
      Command::Read(index) => {
        file.seek(SeekFrom::Start(get_offset(*index, N)))?;
        let mut page = Page::new_empty();
        file.read_exact(page.as_mut())?;
        Ok(Some(page))
      }
      Command::Write(index, page) => {
        file.seek(SeekFrom::Start(get_offset(*index, N)))?;
        file.write_all(page.as_ref())?;
        Ok(None)
      }
      Command::Flush => file.sync_all().map(|_| None),
    }
  }
}

pub struct FinderConfig {
  pub path: PathBuf,
  pub batch_delay: Duration,
  pub batch_size: usize,
}

pub struct Finder<const N: usize> {
  io_c: StoppableChannel<Command<N>, Result<Option<Page<N>>>>,
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
    finder.start_io(io_rx)?;
    finder.start_batch(batch_rx);
    Ok(finder)
  }

  fn start_io(&self, rx: ContextReceiver<Command<N>, Result<Option<Page<N>>>>) -> Result {
    let mut file = OpenOptions::new()
      .create(true)
      .read(true)
      .write(true)
      .open(&self.config.path)
      .map_err(Error::IO)?;
    rx.to_done("finder io", 3 * N, move |cmd: Command<N>| {
      cmd.exec(&mut file).map_err(Error::IO)
    });
    Ok(())
  }

  fn start_batch(&self, rx: ContextReceiver<(usize, Page<N>), Result>) {
    let delay = self.config.batch_delay;
    let count = self.config.batch_size;
    let c = self.io_c.clone();
    rx.with_timer(
      "finder batch",
      N * 3,
      delay,
      move |v: &mut Vec<Sender<Result>>,
            timer: &mut Timer,
            o: Option<((usize, Page<N>), Sender<Result>)>| {
        if let Some(((index, page), done)) = o {
          if let Err(err) = c.send_await(Command::Write(index, page)) {
            done.must_send(Err(err));
            timer.check();
            return;
          };

          v.push(done);
          if v.len() < count {
            timer.check();
            return;
          }
        }

        if let Err(_) = c.send_await(Command::Flush) {
          return;
        }

        v.drain(..).for_each(|done| done.must_send(Ok(())));
        timer.reset()
      },
    );
  }

  pub fn read(&self, index: usize) -> Result<Page<N>> {
    let r = self.io_c.send_await(Command::Read(index))?;
    Ok(r.unwrap())
  }

  pub fn write<T>(&self, index: usize, v: &T) -> Result
  where
    T: Serializable<Error, N>,
  {
    let page = v.serialize()?;
    self.io_c.send_await(Command::Write(index, page))?;
    Ok(())
  }

  pub fn batch_write<T>(&self, index: usize, v: &T) -> Result
  where
    T: Serializable<Error, N>,
  {
    let page = v.serialize()?;
    self.batch_c.send_await((index, page))
  }

  pub fn fsync(&self) -> Result {
    self.io_c.send_await(Command::Flush)?;
    Ok(())
  }
}

fn get_offset(index: usize, n: usize) -> u64 {
  (index * n) as u64
}
