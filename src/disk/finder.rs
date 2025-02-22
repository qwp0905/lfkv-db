use std::{
  fs::{File, Metadata, OpenOptions},
  io::{Read, Seek, SeekFrom, Write},
  ops::Mul,
  path::PathBuf,
  sync::Arc,
  time::Duration,
};

use crate::{
  BackgroundThread, BackgroundWork, Error, Page, Result, Serializable, UnwrappedSender,
};

use super::DirectIO;

enum Command<const N: usize> {
  Read(usize),
  Write(usize, Page<N>),
  Flush,
  Metadata,
}
impl<const N: usize> Command<N> {
  fn exec(&self, file: &mut File) -> Result<(Option<Page<N>>, Option<Metadata>)>
  where
    File: IndexedFile<N>,
  {
    match self {
      Command::Read(index) => {
        file.seek_index(*index)?;
        let mut page = Page::new_empty();
        if let Err(err) = file.read_exact(page.as_mut()) {
          match err.kind() {
            std::io::ErrorKind::UnexpectedEof => return Err(Error::NotFound),
            _ => return Err(Error::IO(err)),
          }
        };
        if page.is_empty() {
          return Err(Error::NotFound);
        }

        Ok((Some(page), None))
      }
      Command::Write(index, page) => {
        file.seek_index(*index)?;
        file.write_all(page.as_ref()).map_err(Error::IO)?;
        Ok((None, None))
      }
      Command::Flush => file.sync_all().map(|_| (None, None)).map_err(Error::IO),
      Command::Metadata => file.metadata().map(|m| (None, Some(m))).map_err(Error::IO),
    }
  }
}

pub struct FinderConfig {
  pub path: PathBuf,
  pub batch_delay: Duration,
  pub batch_size: usize,
}

trait IndexedFile<const N: usize> {
  fn seek_index(&mut self, i: usize) -> Result<usize>;
}
impl<const N: usize> IndexedFile<N> for File {
  fn seek_index(&mut self, i: usize) -> Result<usize> {
    self
      .seek(SeekFrom::Start(i.mul(N) as u64))
      .map_err(Error::IO)
      .map(|c| c as usize)
  }
}

pub struct Finder<const N: usize> {
  io_c: Arc<BackgroundThread<Command<N>, Result<(Option<Page<N>>, Option<Metadata>)>>>,
  batch_c: BackgroundThread<(usize, Page<N>), Result>,
}
impl<const N: usize> Finder<N> {
  pub fn open(config: FinderConfig) -> Result<Self> {
    let mut file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .direct_io(&config.path)
      .map_err(Error::IO)?;

    let file_name = config
      .path
      .file_name()
      .unwrap_or(config.path.as_os_str())
      .to_string_lossy()
      .to_string();

    let io_name = format!("{} finder io", file_name);
    let io_c = Arc::new(BackgroundThread::new(
      &io_name,
      N.mul(1000),
      BackgroundWork::no_timeout(move |cmd: Command<N>| cmd.exec(&mut file)),
    ));

    let cloned_c = io_c.clone();
    let mut wait = vec![];

    let batch_name = format!("{} finder batch", file_name);
    let batch_c = BackgroundThread::new(
      &batch_name,
      N.mul(2).mul(config.batch_size),
      BackgroundWork::with_timer(config.batch_delay, move |v| {
        if let Some(((index, page), done)) = v {
          if let Err(err) = cloned_c.send_await(Command::Write(index, page)) {
            done.must_send(Err(err));
            return false;
          }

          wait.push(done);
          if wait.len().lt(&config.batch_size) {
            return false;
          }
        }

        if let Err(_) = cloned_c.send_await(Command::Flush) {
          return false;
        }

        wait.drain(..).for_each(|done| done.must_send(Ok(())));
        true
      }),
    );

    Ok(Self { io_c, batch_c })
  }
}
impl<const N: usize> Finder<N> {
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

  pub fn close(&self) {
    self.batch_c.close();
    self.io_c.close();
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
