
use crate::{disk::Page, serialize::SerializeFrom};

use super::*;

#[test]
fn test_serialize_internal() {
  let mut page = Page::new();
  let node = CursorNode::Internal(InternalNode::new(vec![], vec![10], None));
  page.serialize_from(&node).expect("serialize error");

  let d = page
    .deserialize::<CursorNode>()
    .expect("desiralize error")
    .as_internal()
    .expect("desirialize internal error");

  assert_eq!(d.keys.len(), 0);
  assert_eq!(d.children.len(), 1);
  assert_eq!(d.children[0], 10);
  assert_eq!(d.right, None)
}

#[test]
fn test_serialize_leaf() {
  let mut page = Page::new();

  let key = vec![49, 50, 51];
  let ptr = 100;
  let prev = None;
  let next = Some(1100);

  let node = CursorNode::Leaf(LeafNode::new(vec![(key.clone(), ptr)], next, prev));
  page.serialize_from(&node).expect("serialize error");

  let d = page
    .deserialize::<CursorNode>()
    .expect("desiralize error")
    .as_leaf()
    .expect("desirialize leaf error");
  assert_eq!(d.entries, vec![(key, ptr)]);
  assert_eq!(d.next, next);
  assert_eq!(d.prev, prev);
}

#[test]
fn test_serialize_internal_with_keys_and_right() {
  let mut page = Page::new();
  let node = CursorNode::Internal(InternalNode::new(
    vec![vec![1, 2], vec![3, 4]],
    vec![10, 20, 30],
    Some((99, vec![5, 6])),
  ));
  page.serialize_from(&node).expect("serialize error");

  let d = page
    .deserialize::<CursorNode>()
    .expect("deserialize error")
    .as_internal()
    .expect("as_internal error");

  assert_eq!(d.keys, vec![vec![1, 2], vec![3, 4]]);
  assert_eq!(d.children, vec![10, 20, 30]);
  assert_eq!(d.right, Some((99, vec![5, 6])));
}

#[test]
fn test_serialize_leaf_with_prev_and_next() {
  let mut page = Page::new();
  let node = CursorNode::Leaf(LeafNode::new(
    vec![(vec![1], 10), (vec![2], 20)],
    Some(50),
    Some(40),
  ));
  page.serialize_from(&node).expect("serialize error");

  let d = page
    .deserialize::<CursorNode>()
    .expect("deserialize error")
    .as_leaf()
    .expect("as_leaf error");

  assert_eq!(d.entries, vec![(vec![1], 10), (vec![2], 20)]);
  assert_eq!(d.prev, Some(40));
  assert_eq!(d.next, Some(50));
}

#[test]
fn test_serialize_empty_leaf() {
  let mut page = Page::new();
  let node = CursorNode::Leaf(LeafNode::new(vec![], None, None));
  page.serialize_from(&node).expect("serialize error");

  let d = page
    .deserialize::<CursorNode>()
    .expect("deserialize error")
    .as_leaf()
    .expect("as_leaf error");

  assert!(d.entries.is_empty());
  assert_eq!(d.prev, None);
  assert_eq!(d.next, None);
}
