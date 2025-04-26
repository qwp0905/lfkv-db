use std::{
  fs::{Metadata, OpenOptions},
  ops::{Div, Mul},
  path::PathBuf,
  sync::Arc,
};

use crate::{
  Error, Page, Result, SharedWorkThread, SingleWorkThread, ToArc, WorkBuilder,
};

use super::thread::{
  create_flush_thread, create_metadata_thread, create_read_thread, create_write_thread,
};

const DEFAULT_READ_THREADS: usize = 3;
const DEFAULT_WRITE_THREADS: usize = 3;

pub struct RandomAccessDiskConfig {
  pub path: PathBuf,
  pub read_threads: Option<usize>,
  pub write_threads: Option<usize>,
}

pub struct RandomAccessDisk<const N: usize> {
  read_ths: Arc<SharedWorkThread<usize, std::io::Result<Page<N>>>>,
  write_ths: Arc<SharedWorkThread<(usize, Page<N>), std::io::Result<()>>>,
  flush_th: Arc<SingleWorkThread<(), std::io::Result<()>>>,
  meta_th: Arc<SingleWorkThread<(), std::io::Result<Metadata>>>,
}
impl<const N: usize> RandomAccessDisk<N> {
  pub fn open(config: RandomAccessDiskConfig) -> Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(&config.path)
      .map_err(Error::IO)?;

    let read_ths = WorkBuilder::new()
      .name(format!("read {}", config.path.to_string_lossy()))
      .stack_size(N.mul(150))
      .shared(config.read_threads.unwrap_or(DEFAULT_READ_THREADS))
      .build(create_read_thread(&file))?
      .to_arc();

    let write_ths = WorkBuilder::new()
      .name(format!("write {}", config.path.to_string_lossy()))
      .stack_size(N.mul(150))
      .shared(config.write_threads.unwrap_or(DEFAULT_WRITE_THREADS))
      .build(create_write_thread(&file))?
      .to_arc();

    Ok(Self {
      read_ths,
      write_ths,
      flush_th: create_flush_thread(&file, &config.path)?.to_arc(),
      meta_th: create_metadata_thread(&file, &config.path)?.to_arc(),
    })
  }

  pub fn read(&self, index: usize) -> Result<Page<N>> {
    self.read_ths.send_await(index)?.map_err(Error::IO)
  }

  pub fn write(&self, index: usize, page: Page<N>) -> Result {
    self.write_ths.send_await((index, page))?.map_err(Error::IO)
  }

  pub fn fsync(&self) -> Result {
    self.flush_th.send_await(())?.map_err(Error::IO)
  }

  pub fn close(&self) {
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
  use tempfile::tempdir;

  const TEST_PAGE_SIZE: usize = 4096;

  #[test]
  fn test_basic_operations() -> Result<()> {
    let dir = tempdir().map_err(Error::IO)?;
    let config = RandomAccessDiskConfig {
      path: dir.path().join("test.db"),
      read_threads: None,
      write_threads: None,
    };
    let finder = RandomAccessDisk::<TEST_PAGE_SIZE>::open(config)?;

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
    let config = RandomAccessDiskConfig {
      path: dir.path().join("test.db"),
      read_threads: None,
      write_threads: None,
    };
    let finder = RandomAccessDisk::<TEST_PAGE_SIZE>::open(config)?;

    // Attempt to read a non-existent page
    let page = finder.read(100)?;
    assert_eq!(page.as_ref(), &[0u8; TEST_PAGE_SIZE]);

    finder.close();
    Ok(())
  }

  #[test]
  fn test_multiple_pages() -> Result<()> {
    let dir = tempdir().map_err(Error::IO)?;
    let config = RandomAccessDiskConfig {
      path: dir.path().join("test.db"),
      read_threads: None,
      write_threads: None,
    };
    let finder = RandomAccessDisk::<TEST_PAGE_SIZE>::open(config)?;

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
    let config = RandomAccessDiskConfig {
      path: dir.path().join("test.db"),
      read_threads: Some(8),
      write_threads: Some(8),
    };

    let finder = Arc::new(RandomAccessDisk::<TEST_PAGE_SIZE>::open(config)?);

    const THREADS_COUNT: usize = 1000;
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
        }
        Ok(())
      }));
    }
    finder.fsync()?;

    // Wait for all write operations to complete
    for handle in handles {
      handle.join().unwrap()?;
    }

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
    let config = RandomAccessDiskConfig {
      path: dir.path().join("test.db"),
      read_threads: None,
      write_threads: None,
    };

    let finder = Arc::new(RandomAccessDisk::<TEST_PAGE_SIZE>::open(config)?);

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
