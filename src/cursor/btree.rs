use std::{
  collections::{BinaryHeap, VecDeque},
  mem::replace,
  ops::Bound,
};

use crate::{
  blob::BlobAppendGuard,
  cache::RefedSlot,
  disk::Pointer,
  objects::{
    BTreeNode, BTreeNodeView, DataEntry, DataEntryView, FindSlotResult, InternalNode,
    NodeFindResult, RecordData, RecordDataView, StaticKey, StaticKeyRef, TreeHeader,
    VersionRecord, HEADER_POINTER, LARGE_VALUE,
  },
  table::{ReserveGuard, TableHandleRef},
  wal::TxId,
  Error, Result,
};

use super::{
  BTreeIter, BTreeRevIter, BufferedValue, CreatablePolicy, KVSnapshot, ReadonlyPolicy,
  ResolvedConflict, Snapshotter, VecRef, WritablePolicy,
};

/**
 * Policy-driven B-link tree index implementation.
 *
 * `BTreeIndex` owns the tree access algorithms: traversal, visible-version
 * lookup, insert/update/delete, snapshot application, and split propagation.
 * Concrete transaction, cache, WAL, and blob behavior is supplied by `Policy`,
 * so this layer only knows how to operate the tree structure.
 */
pub struct BTreeIndex<Policy>(Policy);
impl<Policy> BTreeIndex<Policy> {
  pub const fn new(policy: Policy) -> Self {
    Self(policy)
  }
}
impl<Policy: ReadonlyPolicy> BTreeIndex<Policy> {
  fn get_root(&self, table: &TableHandleRef) -> Result<Pointer> {
    let root = self
      .0
      .fetch_slot(HEADER_POINTER, table)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();
    Ok(root)
  }
  pub fn get(&self, key: StaticKeyRef, table: &TableHandleRef) -> Result<GetResult> {
    let mut ptr = self
      .0
      .fetch_slot(HEADER_POINTER, table)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    loop {
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      match slot.as_ref().view::<BTreeNodeView>()? {
        BTreeNodeView::Internal(node) => ptr = node.find(key)?.unwrap_or_else(|i| i),
        BTreeNodeView::Leaf(node) => match node.find(key)? {
          NodeFindResult::NotFound(_) => return Ok(GetResult::Absent),
          NodeFindResult::Move(next) => ptr = next,
          NodeFindResult::Found(_, record, entry_ptr) => {
            if !self.0.is_visible(record.owner, record.version) {
              match entry_ptr {
                Some(p) => break ptr = p,
                None => return Ok(GetResult::Deleted),
              }
            }
            return Ok(match record.data {
              RecordDataView::Data(range) => {
                GetResult::Present(VecRef::refed(slot, range))
              }
              RecordDataView::Blob(id, offset, len) => {
                GetResult::Present(VecRef::copied(self.0.read_blob(id, offset, len)?))
              }
              RecordDataView::Tombstone => GetResult::Deleted,
            });
          }
        },
      }
    }

    let mut next = Some(ptr);
    while let Some(ptr) = next.take() {
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      let entry: DataEntryView = slot.as_ref().view()?;

      if let Some(record) =
        entry.find(|record| self.0.is_visible(record.owner, record.version))?
      {
        return Ok(match record.data {
          RecordDataView::Data(range) => GetResult::Present(VecRef::refed(slot, range)),
          RecordDataView::Blob(id, offset, len) => {
            GetResult::Present(VecRef::copied(self.0.read_blob(id, offset, len)?))
          }
          RecordDataView::Tombstone => GetResult::Deleted,
        });
      }

      next = entry.get_next();
    }

    Ok(GetResult::Absent)
  }

  pub fn lookup(
    &self,
    key: StaticKeyRef,
    table: &TableHandleRef,
  ) -> Result<LookupResult> {
    let mut ptr = self
      .0
      .fetch_slot(HEADER_POINTER, table)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    loop {
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      match slot.as_ref().view::<BTreeNodeView>()? {
        BTreeNodeView::Internal(node) => ptr = node.find(key)?.unwrap_or_else(|i| i),
        BTreeNodeView::Leaf(node) => match node.find(key)? {
          NodeFindResult::NotFound(_) => return Ok(LookupResult::Absent),
          NodeFindResult::Move(next) => ptr = next,
          NodeFindResult::Found(_, record, entry_ptr) => {
            if self.0.is_visible(record.owner, record.version) {
              if record.data.is_tombstone() {
                return Ok(LookupResult::Deleted);
              }
              return Ok(LookupResult::Present);
            }
            match entry_ptr {
              Some(p) => break ptr = p,
              None => return Ok(LookupResult::Absent),
            }
          }
        },
      }
    }

    let mut next = Some(ptr);
    while let Some(ptr) = next.take() {
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      let entry: DataEntryView = slot.as_ref().view()?;
      if let Some(record) =
        entry.find(|record| self.0.is_visible(record.owner, record.version))?
      {
        if record.data.is_tombstone() {
          return Ok(LookupResult::Deleted);
        }
        return Ok(LookupResult::Present);
      };

      next = entry.get_next();
    }

    Ok(LookupResult::Absent)
  }

  pub fn contains(&self, key: StaticKeyRef, table: &TableHandleRef) -> Result<bool> {
    Ok(matches!(self.lookup(key, table)?, LookupResult::Present))
  }

  fn fill_stack_from(
    &self,
    key: StaticKeyRef,
    table: &TableHandleRef,
    start: Pointer,
    stack: &mut Vec<(Option<StaticKey>, Pointer)>,
  ) -> Result<Pointer> {
    let mut ptr = start;
    while let BTreeNodeView::Internal(node) = self
      .0
      .fetch_slot(ptr, table)?
      .for_read()
      .as_ref()
      .view::<BTreeNodeView>()?
    {
      match node.find(key)? {
        Ok(p) => stack.push((
          node.get_right_key().map(|k| k.to_vec()),
          replace(&mut ptr, p),
        )),
        Err(p) => ptr = p,
      }
    }
    Ok(ptr)
  }

  /**
   * The stack stores one internal-node anchor per level, not a perfectly stable
   * parent path. Split propagation rechecks each level and follows B-link right
   * moves again, so the stack only needs enough information to restart propagation
   * from the relevant levels.
   */
  fn find_leaf_stack(
    &self,
    key: StaticKeyRef,
    table: &TableHandleRef,
  ) -> Result<(Pointer, Vec<Pointer>)> {
    let (mut ptr, height) = {
      let header = self
        .0
        .fetch_slot(HEADER_POINTER, table)?
        .for_read()
        .as_ref()
        .deserialize::<TreeHeader>()?;
      (header.get_root(), header.get_height())
    };
    let mut stack = vec![];

    while let BTreeNodeView::Internal(node) = self
      .0
      .fetch_slot(ptr, table)?
      .for_read()
      .as_ref()
      .view::<BTreeNodeView>()?
    {
      match node.find(key)? {
        Ok(i) => stack.push(replace(&mut ptr, i)),
        Err(i) => ptr = i,
      }
    }

    debug_assert_eq!(height, stack.len() as u16);
    Ok((ptr, stack))
  }

  pub fn range(
    &self,
    table: &TableHandleRef,
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
  ) -> Result<BTreeIter<&'_ Policy>> {
    BTreeIter::open(&self.0, table, start, end)
  }
  pub fn range_rev(
    &self,
    table: &TableHandleRef,
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
  ) -> Result<BTreeRevIter<&'_ Policy>> {
    BTreeRevIter::open(&self.0, table, start, end)
  }
}

impl<Policy: ReadonlyPolicy + Clone> BTreeIndex<Policy> {
  pub fn snapshot(&self, table: &TableHandleRef) -> Result<Snapshotter<Policy>> {
    Snapshotter::open(self.0.clone(), table)
  }
}

impl<Policy: WritablePolicy + Sync> BTreeIndex<Policy> {
  pub fn initialize(&self, table: &TableHandleRef) -> Result {
    let root = self.0.alloc_and_log(&BTreeNode::initial_state(), table)?;
    self
      .0
      .alloc_slot(HEADER_POINTER, table)?
      .for_write()
      .mutate(|slot| {
        self
          .0
          .serialize_and_log(slot, &TreeHeader::new(root), table)
      })?;
    Ok(())
  }

  /**
   * Apply one propagated split to an internal level. The stack entry is only an
   * anchor; the function follows B-link right moves until it reaches the node that
   * should receive the separator. If that node splits, return the next separator
   * for the parent level. Otherwise propagation stops.
   */
  fn apply_split(
    &self,
    evicted_key: StaticKey,
    evicted_ptr: Pointer,
    current: Pointer,
    table: &TableHandleRef,
  ) -> Result<Option<(StaticKey, Pointer)>> {
    enum State {
      Move(Pointer),
      Inserted,
      Exceeded(StaticKey, Pointer),
    }

    let mut ptr = current;
    loop {
      let state = self.0.fetch_slot(ptr, table)?.for_write().mutate(|slot| {
        let mut node = slot.as_ref().deserialize::<BTreeNode>()?;
        let internal = node.as_internal_mut()?;
        if let Err(i) = internal.insert_or_next(&evicted_key, evicted_ptr) {
          return Ok(State::Move(i));
        };

        let Some((split_node, split_key)) = internal.split_if_needed() else {
          self.0.serialize_and_log(slot, &node, table)?;
          return Ok(State::Inserted);
        };

        let split_ptr = self.0.alloc_and_log(&split_node.into_node(), table)?;
        internal.set_right(&split_key, split_ptr);
        self.0.serialize_and_log(slot, &node, table)?;

        Ok(State::Exceeded(split_key, split_ptr))
      })?;
      match state {
        State::Move(i) => ptr = i,
        State::Inserted => return Ok(None),
        State::Exceeded(key, ptr) => return Ok(Some((key, ptr))),
      }
    }
  }

  pub fn recovery_half_split(
    &mut self,
    mut split_key: StaticKey,
    mut split_pointer: Pointer,
    level: u16,
    table: &TableHandleRef,
  ) -> Result {
    let mut header = self
      .0
      .fetch_slot(HEADER_POINTER, table)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?;

    let mut ptr = header.get_root();
    let height = (header.get_height() - level) as usize;

    let mut stack = vec![];
    while stack.len() < height {
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      let node = slot.as_ref().view::<BTreeNodeView>()?.into_internal()?;
      match node.find(&split_key)? {
        Ok(i) => stack.push(replace(&mut ptr, i)),
        Err(i) => ptr = i,
      }
    }

    while let Some(ptr) = stack.pop() {
      let Some((k, p)) = self.apply_split(split_key, split_pointer, ptr, table)? else {
        return Ok(());
      };

      (split_key, split_pointer) = (k, p);
    }

    let new_root = InternalNode::initialize(split_key, header.get_root(), split_pointer);
    let new_root_ptr = self.0.alloc_and_log(&new_root.into_node(), table)?;

    header.set_root(new_root_ptr);
    header.increase_height();
    self
      .0
      .fetch_slot(HEADER_POINTER, table)?
      .for_write()
      .mutate(|slot| self.0.serialize_and_log(slot, &header, table))?;
    Ok(())
  }

  fn propagate_split(
    &self,
    mut split_key: StaticKey,
    mut split_pointer: Pointer,
    mut stack: Vec<Pointer>,
    table: &TableHandleRef,
  ) -> Result {
    // CAS loop: multiple concurrent splits may race to update the root.
    loop {
      let old_height = stack.len() as u16;
      while let Some(ptr) = stack.pop() {
        match self.apply_split(split_key, split_pointer, ptr, table)? {
          Some((k, p)) => (split_key, split_pointer) = (k, p),
          None => return Ok(()),
        }
      }

      let Some((mut ptr, diff)) = self
        .0
        .fetch_slot(HEADER_POINTER, table)?
        .for_write()
        .mutate(|header_slot| {
        let mut header: TreeHeader = header_slot.as_ref().deserialize()?;
        let current_height = header.get_height();
        let ptr = header.get_root();
        if old_height != current_height {
          return Ok(Some((ptr, (current_height - old_height) as usize)));
        }

        let new_root = InternalNode::initialize(split_key.clone(), ptr, split_pointer);
        let new_root_ptr = self.0.alloc_and_log(&new_root.into_node(), table)?;

        header.set_root(new_root_ptr);
        header.increase_height();
        self.0.serialize_and_log(header_slot, &header, table)?;
        Ok(None)
      })?
      else {
        return Ok(());
      };

      while stack.len() < diff {
        let slot = self.0.fetch_slot(ptr, table)?.for_read();
        let node = slot.as_ref().view::<BTreeNodeView>()?.into_internal()?;
        match node.find(&split_key)? {
          Ok(i) => stack.push(replace(&mut ptr, i)),
          Err(i) => ptr = i,
        }
      }
    }
  }

  /**
   * Large values are written to blob storage before the tree record is updated.
   * Keep the append guard alive until the record containing the blob reference has
   * been serialized/logged, so a filled blob segment cannot become readonly and
   * GC-visible in the gap.
   */
  fn create_record(
    &self,
    op: WriteOp,
  ) -> Result<(RecordData, Option<BlobAppendGuard<'_>>)> {
    let WriteOp::Insert(data) = op else {
      return Ok((RecordData::Tombstone, None));
    };
    if data.len() <= LARGE_VALUE {
      return Ok((RecordData::Data(data), None));
    }
    let guard = self.0.write_blob(data)?;
    let data = RecordData::Blob(guard.get_id(), guard.get_offset(), guard.get_len());
    Ok((data, Some(guard)))
  }

  /**
   * Append a snapshot version to the end of an existing data-entry chain.
   *
   * The caller guarantees ordering: the supplied record belongs after the records
   * already present for this key.
   */
  fn apply_version_snapshot(
    &self,
    entry_ptr: Pointer,
    mut record: VersionRecord,
    table: &TableHandleRef,
  ) -> Result {
    enum State {
      Move(Pointer, VersionRecord),
      Inserted,
    }

    let mut ptr = entry_ptr;
    loop {
      let state = self.0.fetch_slot(ptr, table)?.for_write().mutate(|slot| {
        let mut entry: DataEntry = slot.as_ref().deserialize()?;
        if entry.is_available(&record) {
          entry.attach_back(record);
          self.0.serialize_and_log(slot, &entry, table)?;
          return Ok(State::Inserted);
        }

        if let Some(next) = entry.get_next() {
          return Ok(State::Move(next, record));
        }

        let new_entry = DataEntry::init(record, None);
        entry.set_next(self.0.alloc_and_log(&new_entry, table)?);
        self.0.serialize_and_log(slot, &entry, table)?;
        Ok(State::Inserted)
      })?;
      match state {
        State::Move(i, r) => (ptr, record) = (i, r),
        State::Inserted => return Ok(()),
      }
    }
  }

  fn apply_snapshot_one(
    &self,
    snapshot: KVSnapshot,
    table: &TableHandleRef,
    mut ptr: Pointer,
    stack: Vec<Pointer>,
  ) -> Result<(Pointer, Option<StaticKey>)> {
    enum State {
      Move(Pointer, VersionRecord),
      Break(Option<StaticKey>),
      Split(StaticKey, Pointer),
      Apply(Pointer, VersionRecord, Option<StaticKey>),
    }

    let key = snapshot.key;
    let mut record = VersionRecord::new(
      snapshot.owner,
      snapshot.version,
      match snapshot.value {
        BufferedValue::Data(data) => RecordData::Data(data.to_vec()),
        BufferedValue::Blob(id, offset, len) => RecordData::Blob(id, offset, len),
      },
    );

    loop {
      let state = self.0.fetch_slot(ptr, table)?.for_write().mutate(|slot| {
        let leaf = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;
        let mut node = match leaf.find(&key)? {
          NodeFindResult::Move(next) => return Ok(State::Move(next, record)),
          NodeFindResult::Found(pos, old, entry_ptr) => {
            if let Some(p) = entry_ptr {
              let right_key = leaf.clone_next_key();
              return Ok(State::Apply(p, record, right_key));
            }

            if !self.0.is_aborted(old.owner) {
              if table.is_reserved(&key) {
                return Ok(State::Move(ptr, record));
              }

              let mut node = leaf.into_owned()?;
              let entry_ptr = self
                .0
                .alloc_and_log(&DataEntry::init(record, None), table)?;
              node.alloc_entry_at(pos, entry_ptr);
              let right_key = node.clone_next_key();
              self.0.serialize_and_log(slot, &node.into_node(), table)?;
              return Ok(State::Break(right_key));
            }

            let mut node = leaf.into_owned()?;
            node.replace_at(pos, record);
            node
          }
          NodeFindResult::NotFound(pos) => {
            let mut node = leaf.into_owned()?;
            node.insert_at(pos, key.to_vec(), record);
            node
          }
        };

        let Some(split) = node.split_if_needed() else {
          let right_key = node.clone_next_key();
          self.0.serialize_and_log(slot, &node.into_node(), table)?;
          return Ok(State::Break(right_key));
        };

        let mid_key = split.top().clone();
        let split_ptr = self.0.alloc_and_log(&split.into_node(), table)?;

        node.set_next(mid_key.clone(), split_ptr);
        self.0.serialize_and_log(slot, &node.into_node(), table)?;
        Ok(State::Split(mid_key, split_ptr))
      })?;

      match state {
        State::Move(p, o) => (ptr, record) = (p, o),
        State::Break(rk) => return Ok((ptr, rk)),
        State::Split(k, p) => {
          self.propagate_split(k.clone(), p, stack, table)?;
          return Ok((ptr, Some(k)));
        }
        State::Apply(entry_ptr, r, rk) => {
          self.apply_version_snapshot(entry_ptr, r, table)?;
          return Ok((ptr, rk));
        }
      }
    }
  }

  /**
   * Apply a snapshots record produced by the compaction/snapshot path.
   *
   * This is not the normal transaction write path. The caller guarantees that the
   * snapshot record belongs at the end of the key's version chain, so existing
   * records are extended with `attach_back` semantics instead of conflict-checked
   * transaction update semantics.
   */
  pub fn apply_snapshot_bulk(
    &self,
    bulk: Vec<KVSnapshot>,
    table: &TableHandleRef,
  ) -> Result {
    let mut stack = Vec::<(Option<StaticKey>, Pointer)>::new();
    let mut bulk = VecDeque::from(bulk);
    while let Some(snapshot) = bulk.pop_front() {
      let current: StaticKeyRef = &snapshot.key;
      while stack
        .pop_if(|(k, _)| k.as_deref().is_some_and(|k| k <= current))
        .is_some()
      {}

      let start = match stack.pop() {
        Some((_, p)) => p,
        None => self.get_root(table)?,
      };
      let ptr = self.fill_stack_from(current, table, start, &mut stack)?;

      let s = stack.iter().map(|(_, p)| *p).collect::<Vec<_>>();
      let (mut leaf_ptr, mut right_key) =
        self.apply_snapshot_one(snapshot, table, ptr, s.clone())?;

      while let Some(snapshot) =
        bulk.pop_front_if(|s| right_key.as_deref().is_none_or(|r| &*s.key < r))
      {
        (leaf_ptr, right_key) =
          self.apply_snapshot_one(snapshot, table, leaf_ptr, s.clone())?;
      }
    }
    Ok(())
  }
}
impl<Policy: CreatablePolicy + Sync> BTreeIndex<Policy> {
  /**
   * Copy the old leaf record into the data-entry version chain.
   */
  fn copy_old_record(
    &self,
    entry_ptr: Pointer,
    old: VersionRecord,
    table: &TableHandleRef,
  ) -> Result {
    self
      .0
      .fetch_slot(entry_ptr, table)?
      .for_write()
      .mutate(|slot| {
        let mut entry: DataEntry = slot.as_ref().deserialize()?;
        if entry.is_available(&old) {
          entry.attach_front(old);
          self.0.serialize_and_log(slot, &entry, table)?;
          return Ok(());
        }

        let new_entry_ptr = self.0.alloc_and_log(&entry, table)?;
        let new_entry = DataEntry::init(old, Some(new_entry_ptr));
        self.0.serialize_and_log(slot, &new_entry, table)?;
        Ok(())
      })
  }

  fn try_append_at_leaf<'a>(
    &'a self,
    slot: &mut RefedSlot,
    key: StaticKeyRef,
    op: WriteOp,
    table: &'a TableHandleRef,
    create: bool,
  ) -> Result<InsertState<'a>> {
    let leaf = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;
    let (mut node, pos, found) = match leaf.find(key)? {
      NodeFindResult::Move(i) => return Ok(InsertState::Move(i, op)),
      NodeFindResult::Found(pos, old, entry_ptr) => {
        let writable = self.0.is_owned(old.owner) || self.0.is_aborted(old.owner);
        let visible = self.0.is_readable(old.version) && !self.0.is_active(old.owner);
        match (writable, visible) {
          (true, _) => (leaf.into_owned()?, pos, true),
          (false, false) => return Ok(InsertState::Conflict(old.owner, op)),
          (false, true) => {
            return Ok(match table.reserve(key.to_vec(), self.0.current_owner()) {
              Ok(g) => {
                let old = old.into_owned_with(slot.as_ref());
                InsertState::CopyOld(CopyOld::new(g, entry_ptr, old, op))
              }
              Err(i) => InsertState::Conflict(i, op),
            })
          }
        }
      }
      NodeFindResult::NotFound(pos) => {
        if !create {
          let right_key = leaf.clone_next_key();
          return Ok(InsertState::Break(WriteResult::not_matched(), right_key));
        }
        (leaf.into_owned()?, pos, false)
      }
    };

    let (record, _guard) = self.create_record(op)?;
    let new_record =
      VersionRecord::new(self.0.current_owner(), self.0.current_version(), record);
    let mut result = if found {
      node.replace_at(pos, new_record);
      WriteResult::updated(false)
    } else {
      node.insert_at(pos, key.to_vec(), new_record);
      WriteResult::inserted(false)
    };

    let Some(split) = node.split_if_needed() else {
      let right_key = node.clone_next_key();
      self.0.serialize_and_log(slot, &node.into_node(), table)?;
      return Ok(InsertState::Break(result, right_key));
    };
    result.splitted = true;

    let mid_key = split.top().clone();
    let split_ptr = self.0.alloc_and_log(&split.into_node(), table)?;

    node.set_next(mid_key.clone(), split_ptr);
    self.0.serialize_and_log(slot, &node.into_node(), table)?;
    Ok(InsertState::Split(mid_key, split_ptr, result))
  }

  /**
   * Insert, update, or delete a key through an optimistic two-step write protocol.
   *
   * Updating an existing key must preserve the previous leaf record in the
   * data-entry version chain. To avoid coupling the leaf latch with the data-entry
   * latch, the method first copies the old leaf record into the data entry, then
   * retries the leaf update and replaces the latest record.
   */
  fn __insert(
    &self,
    key: StaticKeyRef,
    mut op: WriteOp,
    table: &TableHandleRef,
    create: bool,
    leaf_ptr: Pointer,
    stack: Vec<Pointer>,
  ) -> Result<(WriteResult, Pointer, Option<StaticKey>)> {
    let mut ptr = leaf_ptr;
    loop {
      let state = self
        .0
        .fetch_slot(ptr, table)?
        .for_write()
        .mutate(|slot| self.try_append_at_leaf(slot, key, op, table, create))?;

      match state {
        InsertState::Move(p, o) => (ptr, op) = (p, o),
        InsertState::Break(result, rk) => return Ok((result, ptr, rk)),
        InsertState::Split(k, p, result) => {
          self.propagate_split(k.clone(), p, stack, table)?;
          return Ok((result, ptr, Some(k)));
        }
        InsertState::CopyOld(cmd) => {
          return self.copy_and_update(key, ptr, table, stack, cmd)
        }
        InsertState::Conflict(i, o) => {
          match self.0.resolve_conflict(i) {
            ResolvedConflict::DeadLock => return Err(Error::WriteConflict),
            ResolvedConflict::Closed => {
              if self.0.is_aborted(i) {
                op = o;
                continue;
              }
              return Err(Error::WriteConflict);
            }
          };
        }
      };
    }
  }

  fn copy_and_update(
    &self,
    key: StaticKeyRef,
    mut leaf_ptr: Pointer,
    table: &TableHandleRef,
    stack: Vec<Pointer>,
    copy_old: CopyOld,
  ) -> Result<(WriteResult, Pointer, Option<StaticKey>)> {
    enum State {
      Move(Pointer, WriteOp),
      Break(WriteResult, Option<StaticKey>),
      Split(StaticKey, Pointer, WriteResult),
    }

    let CopyOld {
      insert_guard,
      entry_ptr,
      old_record: old,
      mut operation,
    } = copy_old;

    let entry_ptr = match entry_ptr {
      Some(p) => self.copy_old_record(p, old, table).map(|_| p)?,
      None => self.0.alloc_and_log(&DataEntry::init(old, None), table)?,
    };

    loop {
      let state = self
        .0
        .fetch_slot(leaf_ptr, table)?
        .for_write()
        .mutate(|slot| {
          let mut node = slot.as_ref().deserialize::<BTreeNode>()?;
          let leaf = node.as_leaf_mut()?;
          let pos = match leaf.find_slot(key) {
            FindSlotResult::Replace(i) => i,
            FindSlotResult::Move(next) => return Ok(State::Move(next, operation)),
            FindSlotResult::Insert(_) => unreachable!(),
          };

          let (record, _guard) = self.create_record(operation)?;
          let new_record =
            VersionRecord::new(self.0.current_owner(), self.0.current_version(), record);
          leaf.replace_at(pos, new_record);
          leaf.alloc_entry_at(pos, entry_ptr);

          let Some(split) = leaf.split_if_needed() else {
            let right_key = leaf.clone_next_key();
            self.0.serialize_and_log(slot, &node, table)?;
            return Ok(State::Break(WriteResult::updated(false), right_key));
          };

          let mid_key = split.top().clone();
          let split_ptr = self.0.alloc_and_log(&split.into_node(), table)?;

          leaf.set_next(mid_key.clone(), split_ptr);
          self.0.serialize_and_log(slot, &node, table)?;
          Ok(State::Split(mid_key, split_ptr, WriteResult::updated(true)))
        })?;

      match state {
        State::Move(p, o) => (leaf_ptr, operation) = (p, o),
        State::Break(result, rk) => return Ok((result, leaf_ptr, rk)),
        State::Split(k, p, result) => {
          drop(insert_guard);
          self.propagate_split(k.clone(), p, stack, table)?;
          return Ok((result, leaf_ptr, Some(k)));
        }
      }
    }
  }

  /**
   * Insert a version record, creating the key if it does not exist.
   *
   * `None` creates a tombstone record. This is intentionally distinct from
   * `remove`, which only writes a tombstone when the key already exists.
   */
  pub fn insert_record(
    &self,
    key: StaticKey,
    op: WriteOp,
    table: &TableHandleRef,
  ) -> Result<WriteResult> {
    let (ptr, stack) = self.find_leaf_stack(&key, table)?;
    let (result, _, _) = self.__insert(&key, op, table, true, ptr, stack)?;
    Ok(result)
  }
  pub fn insert(
    &self,
    key: StaticKey,
    data: Vec<u8>,
    table: &TableHandleRef,
  ) -> Result<WriteResult> {
    self.insert_record(key, WriteOp::Insert(data), table)
  }
  pub fn remove(&self, key: StaticKeyRef, table: &TableHandleRef) -> Result<WriteResult> {
    self.insert_if_matched(key, WriteOp::Remove, table)
  }
  /**
   * Update an existing key only.
   *
   * Returns `not_matched` instead of creating a new key when the key is absent.
   */
  pub fn insert_if_matched(
    &self,
    key: StaticKeyRef,
    op: WriteOp,
    table: &TableHandleRef,
  ) -> Result<WriteResult> {
    let (ptr, stack) = self.find_leaf_stack(key, table)?;
    let (result, _, _) = self.__insert(key, op, table, false, ptr, stack)?;
    Ok(result)
  }

  pub fn bulk_executor(
    &self,
    bulk: BulkOp,
    table: &TableHandleRef,
  ) -> BulkExecutor<'_, Policy> {
    BulkExecutor::new(self, bulk, table.clone())
  }
}

pub struct BulkExecutor<'a, Policy> {
  index: &'a BTreeIndex<Policy>,
  bulk: BulkOp,
  table: TableHandleRef,
  stack: Vec<(Option<StaticKey>, Pointer)>,
  current: Option<(Option<StaticKey>, Pointer, Vec<Pointer>)>,
}
impl<'a, Policy> BulkExecutor<'a, Policy> {
  const fn new(
    index: &'a BTreeIndex<Policy>,
    bulk: BulkOp,
    table: TableHandleRef,
  ) -> Self {
    Self {
      index,
      bulk,
      table,
      stack: Vec::new(),
      current: None,
    }
  }

  fn clone_stack(&self) -> Vec<Pointer> {
    self.stack.iter().map(|(_, p)| *p).collect()
  }
}
impl<'a, Policy: CreatablePolicy + Sync> BulkExecutor<'a, Policy> {
  pub fn exec_one(&mut self) -> Result<Option<BulkExecResult>> {
    if self.bulk.is_empty() {
      return Ok(None);
    }

    if let Some((right_key, leaf_ptr, stack)) = self.current.take() {
      if let Some(KeyPair(key, op, create)) = self
        .bulk
        .pop_if(|k| right_key.as_deref().is_none_or(|r| k < r))
      {
        let is_insert = matches!(op, WriteOp::Insert(_));
        let (result, p, rk) =
          self
            .index
            .__insert(&key, op, &self.table, create, leaf_ptr, stack.clone())?;
        self.current = Some((rk, p, stack));
        return Ok(Some(BulkExecResult::new(result, is_insert)));
      }
    };

    let KeyPair(current, op, create) = self.bulk.pop().unwrap_or_else(|| unreachable!());
    while self
      .stack
      .pop_if(|(k, _)| k.as_deref().is_some_and(|k| k <= &current))
      .is_some()
    {}

    let start = match self.stack.pop() {
      Some((_, p)) => p,
      None => self.index.get_root(&self.table)?,
    };

    let leaf_ptr =
      self
        .index
        .fill_stack_from(&current, &self.table, start, &mut self.stack)?;
    let is_insert = matches!(op, WriteOp::Insert(_));
    let stack = self.clone_stack();
    let (result, p, rk) =
      self
        .index
        .__insert(&current, op, &self.table, create, leaf_ptr, stack.clone())?;
    self.current = Some((rk, p, stack));
    Ok(Some(BulkExecResult::new(result, is_insert)))
  }
}

pub enum BulkExecResult {
  Insert(WriteResult),
  Remove(WriteResult),
}
impl BulkExecResult {
  const fn new(result: WriteResult, is_insert: bool) -> Self {
    if is_insert {
      Self::Insert(result)
    } else {
      Self::Remove(result)
    }
  }
}

pub enum LookupResult {
  Absent,
  Deleted,
  Present,
}
pub enum GetResult {
  Absent,
  Deleted,
  Present(VecRef),
}

pub struct WriteResult {
  pub inserted: bool,
  pub updated: bool,
  pub splitted: bool,
}
impl WriteResult {
  const fn inserted(splitted: bool) -> Self {
    Self {
      inserted: true,
      updated: false,
      splitted,
    }
  }
  const fn updated(splitted: bool) -> Self {
    Self {
      inserted: false,
      updated: true,
      splitted,
    }
  }
  const fn not_matched() -> Self {
    Self {
      inserted: false,
      updated: false,
      splitted: false,
    }
  }
}

pub enum WriteOp {
  Insert(Vec<u8>),
  Remove,
}

struct CopyOld<'a> {
  insert_guard: ReserveGuard<'a>,
  entry_ptr: Option<Pointer>,
  old_record: VersionRecord,
  operation: WriteOp,
}
impl<'a> CopyOld<'a> {
  const fn new(
    insert_guard: ReserveGuard<'a>,
    entry_ptr: Option<Pointer>,
    old_record: VersionRecord,
    operation: WriteOp,
  ) -> Self {
    Self {
      insert_guard,
      entry_ptr,
      old_record,
      operation,
    }
  }
}

enum InsertState<'a> {
  Move(Pointer, WriteOp),
  Break(WriteResult, Option<StaticKey>),
  Conflict(TxId, WriteOp),
  Split(StaticKey, Pointer, WriteResult),
  CopyOld(CopyOld<'a>),
}

struct KeyPair(StaticKey, WriteOp, bool);
impl PartialEq for KeyPair {
  fn eq(&self, other: &Self) -> bool {
    self.0.eq(&other.0)
  }
}
impl Eq for KeyPair {}
impl PartialOrd for KeyPair {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(Ord::cmp(self, other))
  }
}
impl Ord for KeyPair {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    other.0.cmp(&self.0)
  }
}
pub struct BulkOp(BinaryHeap<KeyPair>);
impl BulkOp {
  pub const fn new() -> Self {
    Self(BinaryHeap::new())
  }

  pub fn append(&mut self, key: StaticKey, op: WriteOp, create: bool) {
    self.0.push(KeyPair(key, op, create));
  }

  pub fn len(&self) -> usize {
    self.0.len()
  }
  fn is_empty(&self) -> bool {
    self.0.is_empty()
  }

  fn pop_if(&mut self, f: impl FnOnce(StaticKeyRef) -> bool) -> Option<KeyPair> {
    f(self.peek_key()?).then(|| self.0.pop().unwrap_or_else(|| unreachable!()))
  }
  fn pop(&mut self) -> Option<KeyPair> {
    self.0.pop()
  }

  fn peek_key(&self) -> Option<StaticKeyRef<'_>> {
    let KeyPair(k, _, _) = self.0.peek()?;
    Some(k)
  }
}
