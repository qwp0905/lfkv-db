use std::ptr::NonNull;

use super::Bucket;

pub struct LRUList<K, V> {
  head: Option<NonNull<Bucket<K, V>>>,
  tail: Option<NonNull<Bucket<K, V>>>,
  len_: usize,
}
impl<K, V> LRUList<K, V> {
  pub fn new() -> Self {
    Self {
      head: None,
      tail: None,
      len_: 0,
    }
  }

  pub fn clear(&mut self) {
    self.head = None;
    self.tail = None;
    self.len_ = 0;
  }

  pub fn push_head(&mut self, bucket: &mut NonNull<Bucket<K, V>>) {
    self.len_ += 1;
    match &self.tail {
      Some(_) => {
        unsafe { bucket.as_mut() }.set_next(self.head.clone());
        self.head = Some(bucket.clone());
      }
      None => {
        self.tail = Some(bucket.clone());
        self.head = Some(bucket.clone())
      }
    }
  }

  pub fn remove(&mut self, bucket: &mut NonNull<Bucket<K, V>>) {
    if self.len_.eq(&0) {
      return;
    }

    let n = unsafe { bucket.as_mut() }.set_next(None);
    let p = unsafe { bucket.as_mut() }.set_prev(None);

    if let Some(mut next) = &n {
      unsafe { next.as_mut() }.set_prev(p.clone());
    } else {
      self.tail = p.clone();
    }

    if let Some(mut prev) = &p {
      unsafe { prev.as_mut() }.set_next(n);
    } else {
      self.head = n;
    }

    self.len_ -= 1;
  }

  pub fn move_to_head(&mut self, bucket: &mut NonNull<Bucket<K, V>>) {
    self.remove(bucket);
    self.push_head(bucket);
  }

  pub fn pop_tail(&mut self) -> Option<NonNull<Bucket<K, V>>> {
    match self.tail {
      Some(mut tail) => {
        self.remove(&mut tail);
        Some(tail)
      }
      None => None,
    }
  }

  pub fn len(&self) -> usize {
    self.len_
  }
}
