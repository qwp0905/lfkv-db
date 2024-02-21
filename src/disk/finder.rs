use std::{
  fs::{File, OpenOptions},
  io::{Read, Seek, SeekFrom, Write},
  path::PathBuf,
  time::Duration,
};

use crate::{
  AsTimer, BackgroundThread, ContextReceiver, Error, Page, Result, Serializable,
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
  io_c: BackgroundThread<Command<N>, Result<Option<Page<N>>>, 1>,
  batch_c: BackgroundThread<(usize, Page<N>), Result, 1>,
  config: FinderConfig,
}
impl<const N: usize> Finder<N> {
  pub fn open(config: FinderConfig) -> Result<Self> {
    let mut file = OpenOptions::new()
      .create(true)
      .read(true)
      .write(true)
      .open(&config.path)
      .map_err(Error::IO)?;

    let io_c = BackgroundThread::new(
      "finder io",
      move |rx: ContextReceiver<Command<N>, Result<Option<Page<N>>>>| {
        while let Ok((cmd, done)) = rx.recv_done() {
          let r = cmd.exec(&mut file).map_err(Error::IO);
          done.must_send(r);
        }
      },
    );

    let c = io_c.get_channel();

    let delay = config.batch_delay;
    let count = config.batch_size;

    let batch_c = BackgroundThread::new("finder batch", move |rx| {
      let mut v = vec![];
      let mut timer = delay.as_timer();
      while let Ok(o) = rx.recv_done_or_timeout(timer.get_remain()) {
        if let Some(((index, page), done)) = o {
          if let Err(err) = c.send_await(Command::Write(index, page)) {
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

        if let Err(_) = c.send_await(Command::Flush) {
          continue;
        }

        v.drain(..).for_each(|done| done.must_send(Ok(())));
        timer.reset()
      }
    });

    Ok(Self {
      io_c,
      config,
      batch_c,
    })
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
