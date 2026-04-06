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

**Engine design:** tapirs uses a WiscKey-like storage engine for persistent MVCC data. Keys and value pointers live in a two-level LSM tree (L0: unsorted, L1: sorted non-overlapping SSTables). Values live in a separate append-only value log. This separation reduces write amplification -- compaction rewrites only small index entries, not full values. MVCC is built into composite keys: `(K, Timestamp)` allows multiple versions to coexist.

**Write and read paths:** Write path: writes go to an in-memory BTreeMap (memtable), flushed to an L0 SSTable when full. When L0 reaches 4 files, compaction merges L0 into L1. Read path: check memtable first, then L0 (all files), then L1 (binary search). Value log entries include CRC checksums for integrity. The manifest persists LSM state for crash recovery.

**Crash-safe compaction:** When L0 compaction merges SSTables into L1, the manifest is updated atomically via write-fsync-rename: the new manifest is written to a temporary file, fsynced, then renamed over the old manifest. If the process crashes mid-compaction, the old manifest still points to the pre-compaction SSTable set, so incomplete compactions are effectively rolled back on recovery. The old SSTable files are only deleted after the new manifest is durable.

**Value log garbage collection:** The append-only value log accumulates dead entries as values are overwritten or transactions are aborted. GC scans vlog segments and checks each entry's liveness by looking up its key and timestamp in the LSM tree — if the LSM no longer points to that vlog offset, the entry is dead. Live entries are rewritten to a new segment, and the old segment is reclaimed. GC uses `recover_pointers` to skip full deserialization during the liveness scan, and `read_key_ts` for efficient key-timestamp extraction without reading the full value. Key files: `src/mvcc/disk/vlog.rs`, `src/mvcc/disk/gc.rs`.

**Related docs:** See [Storage concepts](../concepts/storage.md) for term definitions, [Testing](testing.md) for FaultyDiskIo, and [Architecture Decisions](architecture-decisions.md) for why WiscKey over plain LSM. Key files: `src/mvcc/disk/lsm.rs`, `src/mvcc/disk/vlog.rs`, `src/mvcc/disk/memtable.rs`, `src/mvcc/disk/manifest.rs`.

| Component | Key file | Description |
|-----------|----------|-------------|
| Memtable | `mvcc/disk/memtable.rs` | In-memory BTreeMap write buffer |
| SSTable | `mvcc/disk/sstable.rs` | Immutable sorted index file |
| LSM | `mvcc/disk/lsm.rs` | Two-level merge tree (L0 unsorted, L1 sorted) |
| Value Log | `mvcc/disk/vlog.rs` | Append-only value storage with CRC |
| Manifest | `mvcc/disk/manifest.rs` | Crash-recovery metadata |
| DiskIo trait | `mvcc/disk/disk_io.rs` | Pluggable I/O (tokio default, io_uring optional) |

## S3 Remote Storage Layer

```
Write Path (flush)              Read Path (lazy download)
    |                                   |
    v                                   v
+----------+                    +----------+
| DiskIo   | (BufferedIo)      | DiskIo   | (S3CachingIo)
| pwrite() |                   | open()   |
+----+-----+                    +----+-----+
     |                               | file missing locally?
     v                               v
+--------------+                +------------------+
| Local file   |                | Download from S3 |
| (len bytes)  |                | (cache locally)  |
+--------------+                +------------------+
     |
     v (sync_to_remote)
+--------------+
| S3 bucket    |
| segments/    |
| manifests/   |
+--------------+
```

**S3CachingIo:** A `DiskIo` implementation that wraps local file I/O with lazy S3 downloads. When `open()` is called and the local file is missing, it downloads the segment from S3 and caches it locally. Subsequent opens use the cached file with zero S3 cost. A process-global registry (keyed by base directory) maps directories to `S3CacheConfig` structs containing bucket, prefix, and endpoint. Used by zero-copy clone and read replicas. Key file: `mvcc/disk/s3_caching_io.rs`.

**ETag cache invalidation:** On download, S3CachingIo saves a `.etag` sidecar file alongside each segment. On re-open with `expected_size` set (from `VlogSegment::open_at`), if the `.etag` file exists, a HEAD request compares ETags. On mismatch, the segment is fully re-downloaded. Sealed segments are immutable so their ETag never changes. Active segments grow between seals so `sync_to_remote` overwrites them on S3.

**BufferedIo len vs capacity:** `AlignedBuf` over-allocates to alignment boundaries. `BufferedIo.pwrite()` writes `buf.len()` bytes (logical data only). `SyncDirectIo`/`UringDirectIo` write `buf.capacity()` bytes (O_DIRECT requires aligned writes). Files on disk match logical size for BufferedIo.

| Component | Key file | Description |
|-----------|----------|-------------|
| S3CachingIo | `mvcc/disk/s3_caching_io.rs` | Lazy-download DiskIo with ETag cache |
| S3StorageConfig | `remote_store/config.rs` | S3 bucket/prefix/endpoint/region config |
| sync_to_remote | `remote_store/sync_to_remote.rs` | Upload new segments + manifest after flush |
