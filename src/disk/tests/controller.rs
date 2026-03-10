use super::*;
use std::sync::Arc;
use std::thread;
use tempfile::tempdir_in;

const TEST_PAGE_SIZE: usize = 4 << 10;

#[test]
fn test_basic_operations() -> Result<()> {
  let dir = tempdir_in(".").map_err(Error::IO)?;
  let config = DiskControllerConfig {
    path: dir.path().join("test.db"),
    thread_count: 3,
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
  let dir = tempdir_in(".").map_err(Error::IO)?;
  let config = DiskControllerConfig {
    path: dir.path().join("test.db"),
    thread_count: 3,
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
  let dir = tempdir_in(".").map_err(Error::IO)?;
  let config = DiskControllerConfig {
    path: dir.path().join("test.db"),
    thread_count: 3,
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
  let dir = tempdir_in(".").map_err(Error::IO)?;
  let config = DiskControllerConfig {
    path: dir.path().join("test.db"),
    thread_count: 20,
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

  let dir = tempdir_in(".").map_err(Error::IO)?;
  let config = DiskControllerConfig {
    path: dir.path().join("test.db"),
    thread_count: 20,
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
          let value = rng.r#gen::<u8>();
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
