use std::{fs::File, io::Result};

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

pub trait Pread {
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
}
impl Pread for File {
  #[cfg(unix)]
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    self.read_at(buf, offset)
  }

  #[cfg(target_os = "windows")]
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    self.seek_read(buf, offset)
  }
}
pub trait Pwrite {
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize>;
}
impl Pwrite for File {
  #[cfg(unix)]
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
    self.write_at(buf, offset)
  }

  #[cfg(windows)]
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
    self.seek_write(buf, offset)
  }
}
// pub trait Append {
//   fn append(&self, buf: &[u8]) -> Result<usize>;
// }
// impl Append for File {
//   fn append(&self, buf: &[u8]) -> Result<usize> {
//     let _lock = FLock::new(self)?;
//     let len = self.metadata()?.len();
//     self.pwrite(buf, len)?;
//     Ok(len as usize)
//   }
// }

// struct FLock<'a>(&'a File);
// impl<'a> FLock<'a> {
//   fn new(file: &'a File) -> Result<Self> {
//     fs2::FileExt::lock_exclusive(file)?;
//     Ok(FLock(file))
//   }
// }
// impl<'a> Drop for FLock<'a> {
//   fn drop(&mut self) {
//     let _ = fs2::FileExt::unlock(self.0);
//   }
// }

#[cfg(test)]
mod tests {
  use super::*;
  use std::fs::{File /* OpenOptions */};
  use std::io::{Read, Write};
  // use std::thread;
  use tempfile::tempdir;

  #[test]
  fn test_pread() -> Result<()> {
    let dir = tempdir()?;
    let file_path = dir.path().join("test_file.txt");
    let content = b"Hello, World!";

    // Create a test file with content
    let mut file = File::create(&file_path)?;
    file.write_all(content)?;
    file.sync_all()?;

    // Open file for reading
    let file = File::open(&file_path)?;

    // Test 1: Normal read
    let mut buf = vec![0; 5];
    let bytes_read = file.pread(&mut buf, 0)?;
    assert_eq!(bytes_read, 5);
    assert_eq!(&buf, b"Hello");

    // Test 2: Read from middle
    let mut buf = vec![0; 5];
    let bytes_read = file.pread(&mut buf, 7)?;
    assert_eq!(bytes_read, 5);
    assert_eq!(&buf, b"World");

    // Test 3: Read beyond file size
    let mut buf = vec![0; 5];
    let bytes_read = file.pread(&mut buf, 20)?;
    assert_eq!(bytes_read, 0);

    // Test 4: Read with empty buffer
    let mut buf = vec![];
    let bytes_read = file.pread(&mut buf, 0)?;
    assert_eq!(bytes_read, 0);

    Ok(())
  }

  #[test]
  fn test_pwrite() -> Result<()> {
    let dir = tempdir()?;
    let file_path = dir.path().join("test_pwrite.txt");

    // Create an empty file
    let file = File::create(&file_path)?;

    // Test 1: Write at the beginning
    let content1 = b"Hello";
    let bytes_written = file.pwrite(content1, 0)?;
    assert_eq!(bytes_written, 5);

    // Test 2: Write at specific offset
    let content2 = b"World";
    let bytes_written = file.pwrite(content2, 6)?;
    assert_eq!(bytes_written, 5);

    // Verify written content
    let mut content = String::new();
    File::open(&file_path)?.read_to_string(&mut content)?;
    assert_eq!(content, "Hello\0World");

    // Test 3: Write with empty buffer
    let empty_buf: &[u8] = &[];
    let bytes_written = file.pwrite(empty_buf, 0)?;
    assert_eq!(bytes_written, 0);

    Ok(())
  }

  // #[test]
  // fn test_flock_basic() -> Result<()> {
  //   let dir = tempdir()?;
  //   let file_path = dir.path().join("test_lock.txt");
  //   let file = File::create(&file_path)?;

  //   // Test lock acquisition
  //   let lock = FLock::new(&file)?;

  //   // Verify lock is acquired by trying to create another lock
  //   let file2 = file.try_clone()?;
  //   assert!(FLock::new(&file2).is_err());

  //   // Drop the first lock
  //   drop(lock);

  //   // Now we should be able to acquire a new lock
  //   let _new_lock = FLock::new(&file2)?;

  //   Ok(())
  // }

  // #[test]
  // fn test_flock_with_append() -> Result<()> {
  //   let dir = tempdir()?;
  //   let file_path = dir.path().join("test_append_lock.txt");
  //   let file = File::create(&file_path)?;

  //   // Test append with automatic locking
  //   file.append(b"Hello")?;
  //   file.append(b" World")?;

  //   // Verify content
  //   let mut content = String::new();
  //   File::open(&file_path)?.read_to_string(&mut content)?;
  //   assert_eq!(content, "Hello World");

  //   Ok(())
  // }

  // #[test]
  // fn test_flock_concurrent() -> Result<()> {
  //   use std::time::Duration;

  //   let dir = tempdir()?;
  //   let file_path = dir.path().join("test_concurrent_lock.txt");
  //   let file = OpenOptions::new()
  //     .create(true)
  //     .write(true)
  //     .read(true)
  //     .open(&file_path)?;

  //   // First thread acquires the lock
  //   let copied = file.try_clone()?;
  //   let handle = thread::spawn(move || -> Result<()> {
  //     let _lock = FLock::new(&copied)?;
  //     thread::sleep(Duration::from_millis(100)); // Hold the lock for a while
  //     Ok(())
  //   });

  //   // Give first thread time to acquire lock
  //   thread::sleep(Duration::from_millis(50));

  //   // Second thread tries to acquire lock while first thread holds it
  //   let file2 = file.try_clone()?;
  //   assert!(FLock::new(&file2).is_err()); // Should fail to acquire lock

  //   handle.join().unwrap()?; // Wait for first thread

  //   // After first thread is done, we should be able to acquire lock
  //   assert!(FLock::new(&file2).is_ok());

  //   Ok(())
  // }

  // #[test]
  // fn test_flock_readonly() -> Result<()> {
  //   let dir = tempdir()?;
  //   let file_path = dir.path().join("test_readonly.txt");

  //   // Create file with some content
  //   let mut file = File::create(&file_path)?;
  //   file.write_all(b"test content")?;

  //   // Open file in read-only mode
  //   let file = File::open(&file_path)?;

  //   // Should be able to acquire lock even on read-only file
  //   let _lock = FLock::new(&file)?;
  //   assert!(file.metadata()?.permissions().readonly());

  //   Ok(())
  // }

  // #[test]
  // fn test_flock_drop_state() -> Result<()> {
  //   let dir = tempdir()?;
  //   let file_path = dir.path().join("test_drop.txt");
  //   let mut file = File::create(&file_path)?;

  //   {
  //     let _lock = FLock::new(&file)?;
  //     // Lock will be dropped here
  //   }

  //   // After drop, we should be able to acquire a new lock
  //   let file2 = File::open(&file_path)?;
  //   assert!(FLock::new(&file2).is_ok());

  //   // Should be able to write to file after lock is dropped
  //   file.write_all(b"test content")?;

  //   Ok(())
  // }

  // #[test]
  // fn test_flock_concurrent_multiple_threads() -> Result<()> {
  //   let dir = tempdir()?;
  //   let file_path = dir.path().join("test_concurrent.txt");
  //   let file = OpenOptions::new()
  //     .create(true)
  //     .write(true)
  //     .read(true)
  //     .open(&file_path)?;

  //   let mut handles = vec![];

  //   // Spawn multiple threads trying to acquire lock simultaneously
  //   for i in 0..10 {
  //     let file = file.try_clone()?;
  //     handles.push(thread::spawn(move || {
  //       thread::sleep(std::time::Duration::from_millis(50)); // Simulate some work
  //       file.append(&[i + 1 as u8]).unwrap();
  //     }));
  //   }

  //   // All threads should complete successfully
  //   for handle in handles {
  //     handle.join().unwrap();
  //   }
  //   file.sync_all()?;

  //   // Should still be able to acquire lock after all threads are done
  //   assert!(FLock::new(&file).is_ok());
  //   let mut content = vec![0; 10];
  //   file.pread(&mut content, 0)?;
  //   for i in 0..9 {
  //     assert_ne!(content[i], content[i + 1]);
  //   }
  //   Ok(())
  // }
}
