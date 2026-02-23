use std::{
  fs::{remove_file, File, Metadata, OpenOptions},
  path::PathBuf,
  sync::Arc,
};

use super::{Page, PagePool, PageRef, Pread, Pwrite};
use crate::{
  error::{Error, Result},
  thread::{SafeWork, SharedWorkThread, WorkBuilder},
  utils::ToArc,
};

enum DiskOperation<const N: usize> {
  Read(u64, PageRef<N>),
  Write(u64, Page<N>),
  Flush,
  Metadata,
}
enum OperationResult<const N: usize> {
  Read(PageRef<N>),
  Write,
  Flush,
  Metadata(Metadata),
}
impl<const N: usize> OperationResult<N> {
  fn as_read(self) -> PageRef<N> {
    match self {
      OperationResult::Read(page) => page,
      _ => unreachable!(),
    }
  }
  fn as_meta(self) -> Metadata {
    match self {
      OperationResult::Metadata(meta) => meta,
      _ => unreachable!(),
    }
  }
}

fn create_thread<'a, const N: usize>(
  file: &'a File,
) -> impl Fn(usize) -> Result<SafeWork<DiskOperation<N>, std::io::Result<OperationResult<N>>>>
     + use<'a, N> {
  |_| {
    let fd = file.try_clone().map_err(Error::IO)?;
    let work = SafeWork::no_timeout(move |operation: DiskOperation<N>| match operation {
      DiskOperation::Read(offset, mut page) => {
        fd.pread(page.as_mut().as_mut(), offset)?;
        Ok(OperationResult::Read(page))
      }
      DiskOperation::Write(offset, page) => {
        fd.pwrite(page.as_ref(), offset)?;
        Ok(OperationResult::Write)
      }
      DiskOperation::Flush => {
        fd.sync_all()?;
        Ok(OperationResult::Flush)
      }
      DiskOperation::Metadata => Ok(OperationResult::Metadata(fd.metadata()?)),
    });
    Ok(work)
  }
}

const DEFAULT_THREADS: usize = 3;

pub struct DiskControllerConfig {
  pub path: PathBuf,
  pub thread_count: Option<usize>,
}

pub struct DiskController<const N: usize> {
  path: PathBuf,
  background:
    Arc<SharedWorkThread<DiskOperation<N>, std::io::Result<OperationResult<N>>>>,
  page_pool: Arc<PagePool<N>>,
}
impl<const N: usize> DiskController<N> {
  pub fn open(config: DiskControllerConfig, page_pool: Arc<PagePool<N>>) -> Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(&config.path)
      .map_err(Error::IO)?;

    let background = WorkBuilder::new()
      .name(format!("disk {}", config.path.to_string_lossy()))
      .stack_size(N * 500)
      .shared(config.thread_count.unwrap_or(DEFAULT_THREADS))
      .build(create_thread(&file))?
      .to_arc();

    Ok(Self {
      path: config.path,
      background,
      page_pool,
    })
  }

  pub fn read(&self, index: usize) -> Result<PageRef<N>> {
    Ok(
      self
        .background
        .send_await(DiskOperation::Read(
          (index * N) as u64,
          self.page_pool.acquire(),
        ))?
        .map_err(Error::IO)?
        .as_read(),
    )
  }

  pub fn write<'a>(&self, index: usize, page: &'a PageRef<N>) -> Result {
    self
      .background
      .send_await(DiskOperation::Write(
        (index * N) as u64,
        page.as_ref().copy(),
      ))?
      .map_err(Error::IO)?;
    Ok(())
  }

  pub fn fsync(&self) -> Result {
    self
      .background
      .send_await(DiskOperation::Flush)?
      .map_err(Error::IO)?;
    Ok(())
  }

  pub fn close(&self) {
    self.background.close();
  }

  pub fn unlink(self) -> Result {
    remove_file(self.path).map_err(Error::IO)
  }

  pub fn len(&self) -> Result<usize> {
    let meta = self
      .background
      .send_await(DiskOperation::Metadata)?
      .map_err(Error::IO)?
      .as_meta();
    Ok((meta.len() as usize) / N)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::Arc;
  use std::thread;
  use tempfile::tempdir;

  const TEST_PAGE_SIZE: usize = 4 << 10;

  #[test]
  fn test_basic_operations() -> Result<()> {
    let dir = tempdir().map_err(Error::IO)?;
    let config = DiskControllerConfig {
      path: dir.path().join("test.db"),
      thread_count: None,
    };
    let page_pool = Arc::new(PagePool::new(16));
    let finder = DiskController::<TEST_PAGE_SIZE>::open(config, page_pool.clone())?;

    // Initial length should be 0
    assert_eq!(finder.len()?, 0);

    // Write a page
    let mut page = page_pool.acquire();
    let test_data = [1u8, 2u8, 3u8, 4u8];
    page.as_mut().as_mut()[..test_data.len()].copy_from_slice(&test_data);
    finder.write(0, &page)?;

    // Immediately write to disk with fsync
    finder.fsync()?;

    // Read the page
    let read_page = finder.read(0)?;
    assert_eq!(&read_page.as_ref().as_ref()[..test_data.len()], &test_data);

    // Check file length
    assert_eq!(finder.len()?, 1);

    finder.close();
    Ok(())
  }

  #[test]
  fn test_nonexistent_page() -> Result<()> {
    let dir = tempdir().map_err(Error::IO)?;
    let config = DiskControllerConfig {
      path: dir.path().join("test.db"),
      thread_count: None,
    };
    let page_pool = Arc::new(PagePool::new(16));
    let finder = DiskController::<TEST_PAGE_SIZE>::open(config, page_pool)?;

    // Attempt to read a non-existent page
    let page = finder.read(100)?;
    assert_eq!(page.as_ref().as_ref(), &[0u8; TEST_PAGE_SIZE]);

    finder.close();
    Ok(())
  }

  #[test]
  fn test_multiple_pages() -> Result<()> {
    let dir = tempdir().map_err(Error::IO)?;
    let config = DiskControllerConfig {
      path: dir.path().join("test.db"),
      thread_count: None,
    };
    let page_pool = Arc::new(PagePool::new(16));
    let co = DiskController::<TEST_PAGE_SIZE>::open(config, page_pool.clone())?;

    // Write multiple pages
    for i in 0..3 {
      let mut page = page_pool.acquire();
      let value = (i + 1) as u8;
      page.as_mut().as_mut()[0] = value;
      co.write(i, &page)?;
    }
    co.fsync()?;

    // Read and verify multiple pages
    for i in 0..3 {
      let page = co.read(i)?;
      assert_eq!(page.as_ref().as_ref()[0], (i + 1) as u8);
    }

    assert_eq!(co.len()?, 3);

    co.close();
    Ok(())
  }

  #[test]
  fn test_concurrent_large_operations() -> Result<()> {
    let dir = tempdir().map_err(Error::IO)?;
    let config = DiskControllerConfig {
      path: dir.path().join("test.db"),
      thread_count: Some(10),
    };
    let page_pool = Arc::new(PagePool::new(16));

    let co = Arc::new(DiskController::<TEST_PAGE_SIZE>::open(
      config,
      page_pool.clone(),
    )?);

    const THREADS_COUNT: usize = 1000;
    const PAGES_PER_THREAD: usize = 25;
    let mut handles = vec![];

    // Perform concurrent write operations from multiple threads
    for thread_id in 0..THREADS_COUNT {
      let finder_clone = co.clone();
      let pool_clone = page_pool.clone();
      handles.push(thread::spawn(move || -> Result<()> {
        let base_idx = thread_id * PAGES_PER_THREAD;

        for i in 0..PAGES_PER_THREAD {
          let page_idx = base_idx + i;
          let mut page = pool_clone.acquire();

          for j in 0..TEST_PAGE_SIZE {
            page.as_mut().as_mut()[j] = ((page_idx + j) % 256) as u8;
          }

          finder_clone.write(page_idx, &page)?;
        }
        Ok(())
      }));
    }
    co.fsync()?;

    // Wait for all write operations to complete
    for handle in handles {
      handle.join().unwrap()?;
    }

    // Verification threads
    let mut verify_handles = vec![];
    for thread_id in 0..THREADS_COUNT {
      let co_clone = co.clone();
      verify_handles.push(thread::spawn(move || -> Result<()> {
        let base_idx = thread_id * PAGES_PER_THREAD;

        for i in 0..PAGES_PER_THREAD {
          let page_idx = base_idx + i;
          let read_page = co_clone.read(page_idx)?;

          for j in 0..TEST_PAGE_SIZE {
            assert_eq!(read_page.as_ref().as_ref()[j], ((page_idx + j) % 256) as u8);
          }
        }
        Ok(())
      }));
    }

    // Wait for all verification to complete
    for handle in verify_handles {
      handle.join().unwrap()?;
    }

    assert_eq!(co.len()?, THREADS_COUNT * PAGES_PER_THREAD);
    co.close();

    Ok(())
  }

  #[test]
  fn test_concurrent_random_operations() -> Result<()> {
    use rand::Rng;

    let dir = tempdir().map_err(Error::IO)?;
    let config = DiskControllerConfig {
      path: dir.path().join("test.db"),
      thread_count: Some(10),
    };
    let page_pool = Arc::new(PagePool::new(16));

    let co = Arc::new(DiskController::<TEST_PAGE_SIZE>::open(
      config,
      page_pool.clone(),
    )?);

    const THREADS_COUNT: usize = 500;
    const OPERATIONS_PER_THREAD: usize = 50;
    const MAX_PAGE_INDEX: usize = 100;

    let mut handles = vec![];

    // Perform random read/write operations from multiple threads
    for _ in 0..THREADS_COUNT {
      let co_clone = co.clone();
      let pool_clone = page_pool.clone();
      handles.push(thread::spawn(move || -> Result<()> {
        let mut rng = rand::thread_rng();

        for _ in 0..OPERATIONS_PER_THREAD {
          let page_idx = rng.gen_range(0..MAX_PAGE_INDEX);

          if rng.gen_bool(0.7) {
            // 70% chance of write operation
            let mut page = pool_clone.acquire();
            let value = rng.gen::<u8>();
            for j in 0..TEST_PAGE_SIZE {
              page.as_mut().as_mut()[j] = value.wrapping_add((j % 256) as u8);
            }
            co_clone.write(page_idx, &page)?;

            if rng.gen_bool(0.2) {
              // 20% chance of fsync
              co_clone.fsync()?;
            }
          } else {
            // 30% chance of read operation
            let _ = co_clone.read(page_idx)?;
          }
        }
        Ok(())
      }));
    }

    // Wait for all operations to complete
    for handle in handles {
      handle.join().unwrap()?;
    }

    co.fsync()?;
    co.close();

    Ok(())
  }
}
