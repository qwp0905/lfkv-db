use std::{
  fs::{File, OpenOptions},
  io::{Read, Seek, SeekFrom, Write},
  path::PathBuf,
  sync::Arc,
  time::Duration,
};

use crate::{
  AsTimer, BackgroundJob, BackgroundThread, ContextReceiver, Error, Page, Result,
  Serializable, StoppableChannel, ThreadPool, UnwrappedReceiver, UnwrappedSender,
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

pub struct F<const N: usize> {
  io_c: BackgroundThread<Command<N>, Result<Option<Page<N>>>>,
  batch_c: BackgroundThread<(usize, Page<N>), Result>,
  config: FinderConfig,
}
impl<const N: usize> F<N> {
  pub fn open(config: FinderConfig) -> Result<Self> {
    let mut file = OpenOptions::new()
      .create(true)
      .read(true)
      .write(true)
      .open(&config.path)
      .map_err(Error::IO)?;

    let io_c = BackgroundThread::new(format!(""), 10, move |rx| {});

    let batch_c = BackgroundThread::new(format!(""), 10, move |rx| {});
    Ok(Self {
      io_c,
      config,
      batch_c,
    })
  }
}

pub struct Finder<const N: usize> {
  io_c: StoppableChannel<Command<N>, Result<Option<Page<N>>>>,
  batch_c: StoppableChannel<(usize, Page<N>), Result>,
  background: Arc<ThreadPool>,
  config: FinderConfig,
}
impl<const N: usize> Finder<N> {
  pub fn open(config: FinderConfig, background: Arc<ThreadPool>) -> Result<Self> {
    let (io_c, rx) = StoppableChannel::new();
    let (batch_c, batch_rx) = StoppableChannel::new();
    let disk = Finder::new(io_c, batch_c, background, config);
    disk.open_file(rx)?;
    disk.start_batch(batch_rx);
    Ok(disk)
  }

  fn new(
    io_c: StoppableChannel<Command<N>, Result<Option<Page<N>>>>,
    batch_c: StoppableChannel<(usize, Page<N>), Result>,
    background: Arc<ThreadPool>,
    config: FinderConfig,
  ) -> Self {
    Self {
      io_c,
      batch_c,
      background,
      config,
    }
  }

  fn open_file(
    &self,
    rx: ContextReceiver<Command<N>, Result<Option<Page<N>>>>,
  ) -> Result {
    let mut file = OpenOptions::new()
      .create(true)
      .read(true)
      .write(true)
      .open(&self.config.path)
      .map_err(Error::IO)?;

    self.background.schedule(move || {
      while let Ok((cmd, done)) = rx.recv_done() {
        let r = cmd.exec(&mut file).map_err(Error::IO);
        done.must_send(r);
      }
    });
    Ok(())
  }

  fn start_batch(&self, rx: ContextReceiver<(usize, Page<N>), Result>) {
    let delay = self.config.batch_delay;
    let count = self.config.batch_size;
    let io_c = self.io_c.clone();
    self.background.schedule(move || {
      let mut v = vec![];
      let mut timer = delay.as_timer();
      while let Ok(o) = rx.recv_done_or_timeout(timer.get_remain()) {
        if let Some(((index, page), done)) = o {
          if let Err(err) = io_c.send_with_done(Command::Write(index, page)).must_recv() {
            done.must_send(Err(err));
            timer.check();
            continue;
          };

          v.push(done);
          if v.len() < count {
            timer.check();
            continue;
          }
        }

        if let Err(_) = io_c.send_with_done(Command::Flush).must_recv() {
          continue;
        }

        v.drain(..).for_each(|done| done.must_send(Ok(())));
        timer.reset()
      }
    });
  }

  pub fn read(&self, index: usize) -> Result<Page<N>> {
    let r = self.io_c.send_with_done(Command::Read(index)).must_recv()?;
    Ok(r.unwrap())
  }

  pub fn write<T>(&self, index: usize, v: &T) -> Result
  where
    T: Serializable<Error, N>,
  {
    let page = v.serialize()?;
    self
      .io_c
      .send_with_done(Command::Write(index, page))
      .must_recv()?;
    Ok(())
  }

  pub fn batch_write<T>(&self, index: usize, v: &T) -> Result
  where
    T: Serializable<Error, N>,
  {
    let page = v.serialize()?;
    self.batch_c.send_with_done((index, page)).must_recv()
  }

  pub fn fsync(&self) -> Result {
    self.io_c.send_with_done(Command::Flush).must_recv()?;
    Ok(())
  }
}

fn get_offset(index: usize, n: usize) -> u64 {
  (index * n) as u64
}
