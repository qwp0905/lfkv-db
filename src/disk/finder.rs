use std::{
  fs::{Metadata, OpenOptions},
  ops::{Div, Mul},
  path::PathBuf,
  sync::Arc,
  time::Duration,
};

use crossbeam::queue::ArrayQueue;

use crate::{
  Error, Page, Result, SafeWork, SharedWorkThread, SingleWorkThread, ToArc,
  UnwrappedSender, WorkBuilder,
};

use super::{Pread, Pwrite};

const DEFAULT_READ_THREADS: usize = 1;
const DEFAULT_WRITE_THREADS: usize = 1;

pub struct FinderConfig {
  pub path: PathBuf,
  pub batch_delay: Duration,
  pub batch_size: usize,
  pub read_threads: Option<usize>,
  pub write_threads: Option<usize>,
}

pub struct Finder<const N: usize> {
  read_ths: Arc<SharedWorkThread<usize, std::io::Result<Page<N>>>>,
  write_ths: Arc<SharedWorkThread<(usize, Page<N>), std::io::Result<()>>>,
  batch_th: Arc<SingleWorkThread<(usize, Page<N>), std::io::Result<()>>>,
  flush_th: Arc<SingleWorkThread<(), std::io::Result<()>>>,
  meta_th: Arc<SingleWorkThread<(), std::io::Result<Metadata>>>,
}
impl<const N: usize> Finder<N> {
  pub fn open(config: FinderConfig) -> Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(&config.path)
      .map_err(Error::IO)?;

    let ff = file.try_clone().map_err(Error::IO)?;
    let flush_th = WorkBuilder::new()
      .name(format!("flush {}", config.path.to_string_lossy()))
      .stack_size(2 << 10)
      .single()
      .no_timeout(move |_| ff.sync_all())
      .to_arc();

    let read_ths = WorkBuilder::new()
      .name(format!("read {}", config.path.to_string_lossy()))
      .stack_size(N.mul(150))
      .shared(config.read_threads.unwrap_or(DEFAULT_READ_THREADS))
      .build(|_| {
        let fd = file.try_clone().map_err(Error::IO)?;
        let work = SafeWork::no_timeout(move |index: usize| {
          let mut page = Page::new();
          fd.pread(page.as_mut(), index.mul(N) as u64)?;
          Ok(page)
        });
        Ok(work)
      })?
      .to_arc();

    let write_ths = WorkBuilder::new()
      .name(format!("write {}", config.path.to_string_lossy()))
      .stack_size(N.mul(150))
      .shared(config.write_threads.unwrap_or(DEFAULT_WRITE_THREADS))
      .build(|_| {
        let fd = file.try_clone().map_err(Error::IO)?;
        let work = SafeWork::no_timeout(move |(index, page): (usize, Page<N>)| {
          fd.pwrite(page.as_ref(), index.mul(N) as u64)?;
          Ok(())
        });
        Ok(work)
      })?
      .to_arc();

    let wc = write_ths.clone();
    let fc = flush_th.clone();
    let wait = ArrayQueue::new(config.batch_size);

    let batch_th = WorkBuilder::new()
      .name(format!("batch {}", config.path.to_string_lossy()))
      .stack_size(N.mul(config.batch_size).mul(20))
      .single()
      .with_timer(config.batch_delay, move |v| {
        if let Some(((index, page), done)) = v {
          match wc.send_await((index, page)) {
            Ok(Err(err)) => {
              done.must_send(Ok(Err(err)));
              return false;
            }
            Err(err) => {
              done.must_send(Err(err));
              return false;
            }
            _ => {}
          };
          let _ = wait.push(done);
          if !wait.is_full() {
            return false;
          }
        }

        match fc.send_await(()) {
          Ok(Err(_)) | Err(_) => return false,
          _ => {}
        }

        while let Some(done) = wait.pop() {
          done.must_send(Ok(Ok(())));
        }

        true
      })
      .to_arc();

    let mf = file.try_clone().map_err(Error::IO)?;
    let meta_th = WorkBuilder::new()
      .name(format!("meta {}", config.path.to_string_lossy()))
      .stack_size(2 << 10)
      .single()
      .no_timeout(move |_| mf.metadata())
      .to_arc();

    Ok(Self {
      read_ths,
      write_ths,
      batch_th,
      flush_th,
      meta_th,
    })
  }

  pub fn read(&self, index: usize) -> Result<Page<N>> {
    self.read_ths.send_await(index)?.map_err(Error::IO)
  }

  pub fn write(&self, index: usize, page: Page<N>) -> Result {
    self.batch_th.send_await((index, page))?.map_err(Error::IO)
  }

  pub fn fsync(&self) -> Result {
    self.flush_th.send_await(())?.map_err(Error::IO)
  }

  pub fn close(&self) {
    self.batch_th.finalize();
    self.write_ths.close();
    self.read_ths.close();
    self.flush_th.close();
  }

  pub fn len(&self) -> Result<usize> {
    let meta = self.meta_th.send_await(())?.map_err(Error::IO)?;
    Ok((meta.len() as usize).div(N))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::Arc;
  use std::thread;
  use std::time::Duration;
  use tempfile::tempdir;

  const TEST_PAGE_SIZE: usize = 4096;

  #[test]
  fn test_basic_operations() -> Result<()> {
    let dir = tempdir().map_err(Error::IO)?;
    let config = FinderConfig {
      path: dir.path().join("test.db"),
      batch_delay: Duration::from_millis(10),
      batch_size: 8,
      read_threads: None,
      write_threads: None,
    };
    let finder = Finder::<TEST_PAGE_SIZE>::open(config)?;

    // Initial length should be 0
    assert_eq!(finder.len()?, 0);

    // Write a page
    let mut page = Page::new();
    let test_data = [1u8, 2u8, 3u8, 4u8];
    page.as_mut()[..test_data.len()].copy_from_slice(&test_data);
    finder.write(0, page)?;

    // Immediately write to disk with fsync
    finder.fsync()?;

    // Read the page
    let read_page = finder.read(0)?;
    assert_eq!(&read_page.as_ref()[..test_data.len()], &test_data);

    // Check file length
    assert_eq!(finder.len()?, 1);

    finder.close();
    Ok(())
  }

  #[test]
  fn test_nonexistent_page() -> Result<()> {
    let dir = tempdir().map_err(Error::IO)?;
    let config = FinderConfig {
      path: dir.path().join("test.db"),
      batch_delay: Duration::from_millis(10),
      batch_size: 8,
      read_threads: None,
      write_threads: None,
    };
    let finder = Finder::<TEST_PAGE_SIZE>::open(config)?;

    // Attempt to read a non-existent page
    let page = finder.read(100)?;
    assert_eq!(page.as_ref(), &[0u8; TEST_PAGE_SIZE]);

    finder.close();
    Ok(())
  }

  #[test]
  fn test_multiple_pages() -> Result<()> {
    let dir = tempdir().map_err(Error::IO)?;
    let config = FinderConfig {
      path: dir.path().join("test.db"),
      batch_delay: Duration::from_millis(10),
      batch_size: 8,
      read_threads: None,
      write_threads: None,
    };
    let finder = Finder::<TEST_PAGE_SIZE>::open(config)?;

    // Write multiple pages
    for i in 0..3 {
      let mut page = Page::new();
      let value = (i + 1) as u8;
      page.as_mut()[0] = value;
      finder.write(i, page)?;
    }
    finder.fsync()?;

    // Read and verify multiple pages
    for i in 0..3 {
      let page = finder.read(i)?;
      assert_eq!(page.as_ref()[0], (i + 1) as u8);
    }

    assert_eq!(finder.len()?, 3);

    finder.close();
    Ok(())
  }

  #[test]
  fn test_concurrent_large_operations() -> Result<()> {
    let dir = tempdir().map_err(Error::IO)?;
    let config = FinderConfig {
      path: dir.path().join("test.db"),
      batch_delay: Duration::from_millis(10),
      batch_size: 8,
      read_threads: None,
      write_threads: None,
    };

    let finder = Arc::new(Finder::<TEST_PAGE_SIZE>::open(config)?);

    const THREADS_COUNT: usize = 100;
    const PAGES_PER_THREAD: usize = 25;
    let mut handles = vec![];

    // Perform concurrent write operations from multiple threads
    for thread_id in 0..THREADS_COUNT {
      let finder_clone = finder.clone();
      handles.push(thread::spawn(move || -> Result<()> {
        let base_idx = thread_id * PAGES_PER_THREAD;

        for i in 0..PAGES_PER_THREAD {
          let mut page = Page::new();
          let page_idx = base_idx + i;

          for j in 0..TEST_PAGE_SIZE {
            page.as_mut()[j] = ((page_idx + j) % 256) as u8;
          }

          finder_clone.write(page_idx, page)?;

          if i % 5 == 0 {
            finder_clone.fsync()?;
          }
        }
        Ok(())
      }));
    }

    // Wait for all write operations to complete
    for handle in handles {
      handle.join().unwrap()?;
    }

    finder.fsync()?;

    // Verification threads
    let mut verify_handles = vec![];
    for thread_id in 0..THREADS_COUNT {
      let finder_clone = finder.clone();
      verify_handles.push(thread::spawn(move || -> Result<()> {
        let base_idx = thread_id * PAGES_PER_THREAD;

        for i in 0..PAGES_PER_THREAD {
          let page_idx = base_idx + i;
          let read_page = finder_clone.read(page_idx)?;

          for j in 0..TEST_PAGE_SIZE {
            assert_eq!(read_page.as_ref()[j], ((page_idx + j) % 256) as u8);
          }
        }
        Ok(())
      }));
    }

    // Wait for all verification to complete
    for handle in verify_handles {
      handle.join().unwrap()?;
    }

    assert_eq!(finder.len()?, THREADS_COUNT * PAGES_PER_THREAD);
    finder.close();

    Ok(())
  }

  #[test]
  fn test_concurrent_random_operations() -> Result<()> {
    use rand::Rng;

    let dir = tempdir().map_err(Error::IO)?;
    let config = FinderConfig {
      path: dir.path().join("test.db"),
      batch_delay: Duration::from_millis(10),
      batch_size: 8,
      read_threads: None,
      write_threads: None,
    };

    let finder = Arc::new(Finder::<TEST_PAGE_SIZE>::open(config)?);

    const THREADS_COUNT: usize = 500;
    const OPERATIONS_PER_THREAD: usize = 50;
    const MAX_PAGE_INDEX: usize = 100;

    let mut handles = vec![];

    // Perform random read/write operations from multiple threads
    for _ in 0..THREADS_COUNT {
      let finder_clone = finder.clone();
      handles.push(thread::spawn(move || -> Result<()> {
        let mut rng = rand::thread_rng();

        for _ in 0..OPERATIONS_PER_THREAD {
          let page_idx = rng.gen_range(0..MAX_PAGE_INDEX);

          if rng.gen_bool(0.7) {
            // 70% chance of write operation
            let mut page = Page::new();
            let value = rng.gen::<u8>();
            for j in 0..TEST_PAGE_SIZE {
              page.as_mut()[j] = value.wrapping_add((j % 256) as u8);
            }
            finder_clone.write(page_idx, page)?;

            if rng.gen_bool(0.2) {
              // 20% chance of fsync
              finder_clone.fsync()?;
            }
          } else {
            // 30% chance of read operation
            let _ = finder_clone.read(page_idx)?;
          }
        }
        Ok(())
      }));
    }

    // Wait for all operations to complete
    for handle in handles {
      handle.join().unwrap()?;
    }

    finder.fsync()?;
    finder.close();

    Ok(())
  }
}
