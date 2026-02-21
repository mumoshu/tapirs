# Storage Internals

```
Write Path                          Read Path
    |                                   |
    v                                   v
+----------+                    +----------+
| Memtable | <-- check first -- | Memtable |
| (BTreeMap)|                   |          |
+----+-----+                    +----+-----+
     | flush                         | miss
     v                               v
+----------+                    +----------+
| L0       | (unsorted)        | L0       | (check all files)
| SSTables |                   | SSTables |
+----+-----+                    +----+-----+
     | compact                       | miss
     v                               v
+----------+                    +----------+
| L1       | (sorted,          | L1       | (binary search)
| SSTables |  non-overlapping) | SSTables |
+----------+                    +----------+

+----------------------------------------------+
| Value Log (append-only, CRC per entry)       |
| Keys in LSM point to offsets in value log    |
+----------------------------------------------+

+----------------------------------------------+
| Manifest (crash-recovery metadata)           |
+----------------------------------------------+
```

**P1 -- Engine design:** tapirs uses a WiscKey-like storage engine for persistent MVCC data. Keys and value pointers live in a two-level LSM tree (L0: unsorted, L1: sorted non-overlapping SSTables). Values live in a separate append-only value log. This separation reduces write amplification -- compaction rewrites only small index entries, not full values. MVCC is built into composite keys: `(K, Timestamp)` allows multiple versions to coexist.

**P2 -- Write and read paths:** Write path: writes go to an in-memory BTreeMap (memtable), flushed to an L0 SSTable when full. When L0 reaches 4 files, compaction merges L0 into L1. Read path: check memtable first, then L0 (all files), then L1 (binary search). Value log entries include CRC checksums for integrity. The manifest persists LSM state for crash recovery.

**P3 -- Related docs:** See [Storage concepts](../concepts/storage.md) for term definitions, [Testing](testing.md) for FaultyDiskIo, and [Architecture Decisions](architecture-decisions.md) for why WiscKey over plain LSM. Key files: `src/mvcc/disk/lsm.rs`, `src/mvcc/disk/vlog.rs`, `src/mvcc/disk/memtable.rs`, `src/mvcc/disk/manifest.rs`.

| Component | Key file | Description |
|-----------|----------|-------------|
| Memtable | `mvcc/disk/memtable.rs` | In-memory BTreeMap write buffer |
| SSTable | `mvcc/disk/sstable.rs` | Immutable sorted index file |
| LSM | `mvcc/disk/lsm.rs` | Two-level merge tree (L0 unsorted, L1 sorted) |
| Value Log | `mvcc/disk/vlog.rs` | Append-only value storage with CRC |
| Manifest | `mvcc/disk/manifest.rs` | Crash-recovery metadata |
| DiskIo trait | `mvcc/disk/disk_io.rs` | Pluggable I/O (tokio default, io_uring optional) |
