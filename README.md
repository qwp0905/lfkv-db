# LFKV-DB

Lock-Free Key-Value Storage Engine implemented in Rust.

A persistent, ACID-compliant key-value store built on a Blink tree index with MVCC (Multi-Version Concurrency Control) for snapshot isolation.

## Features

- **Blink Tree Index** - Lock-free concurrent B-tree variant with right-link pointers for non-blocking traversal and range scans
- **MVCC** - Snapshot isolation through version chains; readers never block writers
- **Write-Ahead Logging** - Segment-based WAL with group commit for durability and crash recovery
- **Buffer Pool** - LRU-based page cache with configurable capacity (defaults to 30% of system memory)
- **Garbage Collection** - Background 4-channel pipeline that reclaims obsolete versions and freed pages
- **Crash Recovery** - Automatic WAL replay on startup with redo of committed transactions

## Usage

```rust
use std::time::Duration;
use lfkv_db::{Engine, EngineConfig};

let engine = Engine::bootstrap(EngineConfig {
    base_path: "./data",
    wal_file_size: 64 * 1024 * 1024,        // 64MB per WAL segment
    checkpoint_interval: Duration::from_secs(30),
    group_commit_delay: Duration::from_millis(5),
    group_commit_count: 16,
    gc_trigger_interval: Duration::from_secs(10),
    gc_trigger_count: 1000,
    buffer_pool_shard_count: 16,
})?;

// Start a transaction
let mut tx = engine.new_transaction()?;

// Insert
tx.insert(b"key1".to_vec(), b"value1".to_vec())?;
tx.insert(b"key2".to_vec(), b"value2".to_vec())?;

// Read
let value = tx.get(&b"key1".to_vec())?;

// Range scan [start, end)
let mut iter = tx.scan(&b"key1".to_vec(), &b"key3".to_vec())?;
while let Some(data) = iter.try_next()? {
    // process data
}

// Full table scan
let mut iter = tx.scan_all()?;
while let Some(data) = iter.try_next()? {
    // process data
}

// Delete
tx.remove(&b"key1".to_vec())?;

// Commit (or auto-aborts on drop)
tx.commit()?;
```

## Architecture

```
                    ┌──────────────────┐
                    │      Engine      │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  TxOrchestrator   │
                    │  (coordination)   │
                    └──┬─────┬──────┬──┘
                       │     │      │
           ┌───────────┘     │      └───────────┐
           │                 │                  │
  ┌────────▼───────┐ ┌──────▼───────┐ ┌────────▼───────┐
  │   Buffer Pool  │ │     WAL      │ │    Version     │
  │  (page cache)  │ │  (durability)│ │  Visibility    │
  │   LRU evict    │ │  group commit│ │   (MVCC)       │
  └────────┬───────┘ └──────┬───────┘ └────────────────┘
           │                │
  ┌────────▼───────┐ ┌──────▼───────┐
  │ DiskController │ │ WAL Segments │
  │   (async I/O)  │ │ + Checkpoint │
  └────────────────┘ └──────────────┘
```

### Modules

| Module | Description |
|--------|-------------|
| `engine` | Bootstrap, configuration, lifecycle management |
| `cursor` | Blink tree traversal, CRUD operations, range iterator, GC |
| `transaction` | Transaction orchestrator, MVCC visibility, free list |
| `buffer_pool` | LRU page cache with sharded locking, dirty page tracking |
| `wal` | Segment-based WAL, log records, group commit, replay |
| `disk` | Async disk I/O controller with read/write thread pools |
| `serialize` | Page-level serialization/deserialization |

### Transaction Lifecycle

1. `Engine::new_transaction()` allocates a transaction ID and registers it as active
2. All reads check MVCC visibility - only committed versions are visible to other transactions
3. Writes append version records to data entries; write-write conflicts trigger auto-abort
4. `commit()` writes a commit record to WAL and calls `fsync` via group commit
5. `drop` auto-aborts uncommitted transactions

### Write-Ahead Logging

- **Segment-based**: WAL files rotate at a configurable size threshold
- **Group commit**: Batches multiple `fsync` calls to amortize I/O cost
- **Checkpoint**: Old WAL segments are handed off to a background checkpoint thread
- **Replay**: On startup, replays uncommitted WAL records and rebuilds transaction state

### Garbage Collection

Background pipeline that:
1. Identifies versions no longer visible to any active transaction
2. Removes obsolete version records from data entries
3. Reclaims freed pages back to the free list
4. Cleans up aborted transaction metadata

## Dependencies

| Crate | Purpose |
|-------|---------|
| `crossbeam` | Lock-free queues, channels, scoped threads |
| `hashbrown` | Raw hash table API for buffer pool page table |
| `chrono` | Timestamp generation for WAL segment naming |
| `sysinfo` | System memory detection for buffer pool sizing |
| `thiserror` | Error type derivation |
| `serde_json` | Log serialization |

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.
