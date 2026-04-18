# Storage Engine

```
  Writes                                    Reads
    |                                         |
    v                                         v
  +-----------+                         +-----------+
  | Memtable  |  (current-view          | Memtable  |  (checked first)
  | BTreeMap  |   raw values)           +-----+-----+
  +-----+-----+                               |  miss
        | seal_view() at view change          v
        v                              +-----------+
  +-----------+  --append--+           | K->VlogPtr|  in-memory index
  | Active    |            |           |  BTreeMap |  (accumulates
  | Vlog Seg  |            |           +-----+-----+   across seals)
  +-----+-----+            |                 |  miss (or SstOnly mode)
        |                  |                 v
        v                  +---> VlogPtr -> +-----------+
  +-----------+                             | SSTs      |  reverse scan;
  | Sealed    |                             | (one per  |  one per seal
  | Vlog Segs | <- read via VlogPtr         |  seal)    |
  +-----------+                             +-----------+

  Keys + VlogPtrs in SSTs / index
  Values in value log
```

**WiscKey architecture:** tapirs persists data using a WiscKey-like storage engine that separates keys from values on disk. The in-memory K→VlogPtr index and its on-disk SSTs store only composite keys mapping to value-log locations, while a separate append-only value log stores the actual values. This separation reduces write amplification: each `seal_view()` flushes the index as a small per-seal SST, rewriting only key pointers, not the full values. The destination of reclamation is shard-level: `tapictl compact shard` migrates a shard onto fresh replicas that receive only committed state, replacing accumulated SSTs and vlog segments wholesale. There is no background in-place compaction.

**MVCC via composite keys:** MVCC (Multi-Version Concurrency Control) is built directly into the storage layer through composite keys: each key is stored as `(K, Timestamp)`, allowing multiple versions of the same key to coexist on disk. When a transaction reads at a specific timestamp, the storage engine returns the latest version at or before that timestamp. This is the foundation that enables TAPIR's concurrent transaction processing — multiple in-flight transactions can read different versions of the same key without blocking each other.

**Deep-dives:** For implementation details (write path, read path, seal checkpoint, S3 caching), see the [Storage internals](../internals/storage.md) deep-dive. For shard-level reclamation of vlog space, see [tapictl compact shard](../../operate/cli-tapictl-compact.md). For why WiscKey was chosen over a plain LSM, see [Architecture Decisions](../internals/architecture-decisions.md). Next: [Resharding](resharding.md). Back to [Concepts](README.md).

| Term | Definition in tapirs | Where it appears |
|------|---------------------|-----------------|
| Value Log (Vlog) | Append-only log of value-log entries with a CRC32 per entry. Values stored separately from the key index to reduce write amplification | `storage/wisckeylsm/vlog.rs` |
| VlogLsm | WiscKey engine: current-view memtable + optional in-memory K→VlogPtr index + flat `Vec<SSTableReader>` (one SST per `seal_view()`). Not a level-based LSM | `storage/wisckeylsm/lsm.rs` |
| Memtable | In-memory write buffer (BTreeMap by default, pluggable via the `Memtable<K, V>` trait). Flushed by `seal_view()` at view change, not on size | `storage/io/memtable.rs` |
| SSTable | Immutable on-disk index file — sorted composite-key entries mapping `CompositeKey(K, TS)` to `VlogPtr`. One written per `seal_view()` | `storage/wisckeylsm/sst.rs` |
| Shard compaction | Migrates a shard to fresh replicas, shipping only committed data. Reclaims accumulated IR-record entries, per-seal SSTs, and fragmented vlog segments as a whole. Not an in-place operation | `sharding/shardmanager/cdc.rs` |
| Tombstone (Storage) | Marker for a deleted key at a specific timestamp — an `IO::Commit` whose write-set value is `None`. Not the same as discovery tombstone | `storage/tapir/store.rs` |
| Manifest | Metadata file persisting engine state — SST list, vlog-segment list, `next_sst_id`, `max_read_time`. Atomically updated (write-fsync-rename) at each `seal_view()` | `storage/wisckeylsm/manifest.rs` |
