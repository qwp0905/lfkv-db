use core::mem;
use core::ptr::NonNull;

#[allow(unused)]
#[derive(Debug)]
pub struct DoubleLinkedListElement<T> {
  next: Option<NonNull<DoubleLinkedListElement<T>>>,
  prev: Option<NonNull<DoubleLinkedListElement<T>>>,
  pub element: T,
}
#[allow(unused)]
impl<T> DoubleLinkedListElement<T> {
  pub fn new(v: T) -> Self {
    Self {
      next: None,
      prev: None,
      element: v,
    }
  }

  pub fn new_ptr(v: T) -> NonNull<Self> {
    NonNull::from(Box::leak(Box::new(DoubleLinkedListElement::new(v))))
  }

  pub fn element(&self) -> &T {
    &self.element
  }
}
impl<T> AsMut<T> for DoubleLinkedListElement<T> {
  fn as_mut(&mut self) -> &mut T {
    &mut self.element
  }
}

#[allow(unused)]
// #[derive(Debug)]
pub struct DoubleLinkedList<T> {
  head: Option<NonNull<DoubleLinkedListElement<T>>>,
  tail: Option<NonNull<DoubleLinkedListElement<T>>>,
  len: usize,
}
// private methods
#[allow(unused)]
impl<T> DoubleLinkedList<T> {
  #[inline]
  #[must_use]
  pub const fn new() -> Self {
    Self {
      head: None,
      tail: None,
      len: 0,
    }
  }
  /// Adds the given node to the front of the list.
  ///
  /// # Safety
  /// `node` must point to a valid node that was boxed and leaked using the list's allocator.
  /// This method takes ownership of the node, so the pointer should not be used again.
  #[inline]
  pub unsafe fn push_front(
    &mut self,
    node: NonNull<DoubleLinkedListElement<T>>,
  ) {
    // This method takes care not to create mutable references to whole nodes,
    // to maintain validity of aliasing pointers into `element`.
    unsafe {
      (*node.as_ptr()).next = self.head;
      (*node.as_ptr()).prev = None;
      let node = Some(node);

      match self.head {
        None => self.tail = node,
        // Not creating new mutable (unique!) references overlapping `element`.
        Some(head) => (*head.as_ptr()).prev = node,
      }

      self.head = node;
      self.len += 1;
    }
  }

  /// Removes and returns the node at the front of the list.
  #[inline]
  pub fn pop_front(&mut self) -> Option<Box<DoubleLinkedListElement<T>>> {
    // This method takes care not to create mutable references to whole nodes,
    // to maintain validity of aliasing pointers into `element`.
    self.head.map(|node| unsafe {
      let node = Box::from_raw(node.as_ptr());
      self.head = node.next;

      match self.head {
        None => self.tail = None,
        // Not creating new mutable (unique!) references overlapping `element`.
        Some(head) => (*head.as_ptr()).prev = None,
      }

      self.len -= 1;
      node
    })
  }

  /// Adds the given node to the back of the list.
  ///
  /// # Safety
  /// `node` must point to a valid node that was boxed and leaked using the list's allocator.
  /// This method takes ownership of the node, so the pointer should not be used again.
  #[inline]
  pub unsafe fn push_back(
    &mut self,
    node: NonNull<DoubleLinkedListElement<T>>,
  ) {
    // This method takes care not to create mutable references to whole nodes,
    // to maintain validity of aliasing pointers into `element`.
    unsafe {
      (*node.as_ptr()).next = None;
      (*node.as_ptr()).prev = self.tail;
      let node = Some(node);

      match self.tail {
        None => self.head = node,
        // Not creating new mutable (unique!) references overlapping `element`.
        Some(tail) => (*tail.as_ptr()).next = node,
      }

      self.tail = node;
      self.len += 1;
    }
  }

  /// Removes and returns the node at the back of the list.
  #[inline]
  pub fn pop_back(&mut self) -> Option<Box<DoubleLinkedListElement<T>>> {
    // This method takes care not to create mutable references to whole nodes,
    // to maintain validity of aliasing pointers into `element`.
    self.tail.map(|node| unsafe {
      let node = Box::from_raw(node.as_ptr());
      self.tail = node.prev;

      match self.tail {
        None => self.head = None,
        // Not creating new mutable (unique!) references overlapping `element`.
        Some(tail) => (*tail.as_ptr()).next = None,
      }

      self.len -= 1;
      node
    })
  }

  /// Unlinks the specified node from the current list.
  ///
  /// Warning: this will not check that the provided node belongs to the current list.
  ///
  /// This method takes care not to create mutable references to `element`, to
  /// maintain validity of aliasing pointers.
  #[inline]
  pub unsafe fn remove(
    &mut self,
    mut node: NonNull<DoubleLinkedListElement<T>>,
  ) {
    let node = unsafe { node.as_mut() }; // this one is ours now, we can create an &mut.

    // Not creating new mutable (unique!) references overlapping `element`.
    match node.prev {
      Some(prev) => unsafe { (*prev.as_ptr()).next = node.next },
      // this node is the head node
      None => self.head = node.next,
    };

    match node.next {
      Some(next) => unsafe { (*next.as_ptr()).prev = node.prev },
      // this node is the tail node
      None => self.tail = node.prev,
    };

    self.len -= 1;
  }

  /// Detaches all nodes from a linked list as a series of nodes.
  #[inline]
  pub fn detach_all(
    mut self,
  ) -> Option<(
    NonNull<DoubleLinkedListElement<T>>,
    NonNull<DoubleLinkedListElement<T>>,
    usize,
  )> {
    let head = self.head.take();
    let tail = self.tail.take();
    let len = mem::replace(&mut self.len, 0);
    if let Some(head) = head {
      // SAFETY: In a DoubleLinked, either both the head and tail are None because
      // the list is empty, or both head and tail are Some because the list is populated.
      // Since we have verified the head is Some, we are sure the tail is Some too.
      let tail = unsafe { tail.unwrap_unchecked() };
      Some((head, tail, len))
    } else {
      None
    }
  }

  #[inline]
  #[must_use]
  pub fn front(&self) -> Option<&T> {
    unsafe { self.head.as_ref().map(|node| node.as_ref().element()) }
  }

  #[inline]
  #[must_use]
  pub fn back(&self) -> Option<&T> {
    unsafe { self.tail.as_ref().map(|node| node.as_ref().element()) }
  }

  #[inline]
  #[must_use]
  pub fn len(&self) -> usize {
    self.len
  }
}
impl<T> Drop for DoubleLinkedList<T> {
  fn drop(&mut self) {
    struct DropGuard<'a, T>(&'a mut DoubleLinkedList<T>);

    impl<'a, T> Drop for DropGuard<'a, T> {
      fn drop(&mut self) {
        // Continue the same loop we do below. This only runs when a destructor has
        // panicked. If another one panics this will abort.
        while self.0.pop_front().is_some() {}
      }
    }

    // Wrap self so that if a destructor panics, we can try to keep looping
    let guard = DropGuard(self);
    while guard.0.pop_front().is_some() {}
    mem::forget(guard);
  }
}

impl<T> std::fmt::Debug for DoubleLinkedList<T>
where
  T: std::fmt::Debug,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str("DoubleLinkedList { ")?;
    let mut node = self.head;
    while let Some(ptr) = node {
      let n = unsafe { ptr.as_ref() };
      n.element.fmt(f)?;
      f.write_str(", ")?;
      node = n.next;
    }
    f.write_str("}")?;
    Ok(())
  }
}
