# Storage Engine

```
  Writes                                        Reads
    |                                             |
    v                                             v
  +----------+                              +----------+
  | Memtable |  (in-memory BTreeMap)        | Memtable |
  +----+-----+                              +----+-----+
       |  flush                                  |  miss
       v                                         v
  +----------+     +----------+             +----------+
  | L0       |     | L0       |             | L0       |  (unsorted SSTables)
  | SSTable  |     | SSTable  |             | SSTable  |
  +----+-----+     +----+-----+             +----+-----+
       |                |                        |  miss
       |   compact      |                        v
       +-------+--------+                  +----------+
               |                           | L1       |  (sorted, merged)
               v                           | SSTable  |
         +----------+                      +----+-----+
         | L1       |                           |
         | SSTable  |                           v
         +----------+                      +---------+
                                           | Value   |  (append-only log)
         Keys only in LSM                  | Log     |
         Values in Vlog -->                +---------+
```

**P1 — WiscKey architecture:** tapirs persists data using a WiscKey-like storage engine that separates keys from values on disk. An LSM (Log-Structured Merge) tree stores sorted index entries — small records mapping composite keys to value locations — while a separate append-only value log stores the actual values. This separation dramatically reduces write amplification: when the LSM tree compacts (merges and deduplicates index entries), it only rewrites small key pointers, not the full values. The result is significantly less disk I/O during compaction compared to a traditional LSM tree that stores keys and values together.

**P2 — MVCC via composite keys:** MVCC (Multi-Version Concurrency Control) is built directly into the storage layer through composite keys: each key is stored as `(K, Timestamp)`, allowing multiple versions of the same key to coexist on disk. When a transaction reads at a specific timestamp, the storage engine returns the latest version at or before that timestamp. This is the foundation that enables TAPIR's concurrent transaction processing — multiple in-flight transactions can read different versions of the same key without blocking each other.

**P3 — Deep-dives:** For implementation details (write path, read path, compaction triggers, crash recovery), see the [Storage internals](../internals/storage.md) deep-dive. For why WiscKey was chosen over a plain LSM, see [Architecture Decisions](../internals/architecture-decisions.md). Next: [Resharding](resharding.md). Back to [Concepts](README.md).

| Term | Definition in tapirs | Where it appears |
|------|---------------------|-----------------|
| Value Log (Vlog) | Append-only log of key-value entries. Values stored separately from LSM index to reduce write amplification | `mvcc/disk/vlog.rs` |
| LSM Tree | Two-level sorted index (L0 unsorted, L1 sorted). Stores composite keys `(K, Timestamp)` pointing to vlog locations | `mvcc/disk/lsm.rs` |
| Memtable | In-memory write buffer (BTreeMap). Flushed to L0 SSTable on size limit | `mvcc/disk/memtable.rs` |
| SSTable | Immutable on-disk index file — sorted key entries mapping `CompositeKey(K, TS)` to `ValuePointer` | `mvcc/disk/sstable.rs` |
| Compaction | Merges L0 into L1 — consolidates duplicates, removes tombstones, triggered when L0 reaches 4 files | `mvcc/disk/lsm.rs` |
| Tombstone (Storage) | Marker for a deleted key at a specific timestamp — not the same as discovery tombstone | `mvcc/store.rs` |
| Manifest | Metadata file persisting LSM state — SSTable lists, next ID, vlog cursor. Replayed on recovery | `mvcc/disk/manifest.rs` |
