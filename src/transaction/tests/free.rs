
use crate::{disk::Page, serialize::SerializeFrom};

use super::*;

#[test]
fn test_free_page_roundtrip() {
  let mut page = Page::new();
  let i = 99;
  let free = FreeBlock::new(Some(i));
  page.serialize_from(&free).expect("serialize error");
  let decoded: FreeBlock = page.deserialize().expect("deserialize error");
  assert_eq!(decoded.prev, Some(i));
  assert_eq!(decoded.next, None);
}

#[test]
fn test_free_page_full() {
  let mut page = Page::new();
  let mut free = FreeBlock::new(None);
  let mut i = 0;
  while !free.is_available() {
    free.list.push(i);
    i += 1;
  }

  page.serialize_from(&free).expect("serialize error");
  let decoded: FreeBlock = page.deserialize().expect("deserialize error");
  assert_eq!(decoded.is_available(), true);

  for c in 0..i {
    assert_eq!(decoded.list[c], c);
  }
  assert_eq!(decoded.list.len(), i);
  assert_eq!(decoded.prev, None);
  assert_eq!(decoded.next, None);
}
