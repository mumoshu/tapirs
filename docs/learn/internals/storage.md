# Storage Internals

```
Write Path                        Read Path
    |                                 |
    v                                 v
+-----------+                   +-----------+
| Memtable  |  (current-view    | Memtable  |  (checked first)
| BTreeMap  |   raw values)     +-----+-----+
+-----+-----+                         | miss
      |                               v
      | seal_view() at          +-----------+
      | view change             | K->VlogPtr|  in-memory index
      v                         |  BTreeMap |  (grows on seal,
+-----------+ --append--+       +-----+-----+   never cleared;
| Active    |           |             | miss    skipped in
| Vlog Seg  |           |             v         SstOnly mode)
+-----+-----+           +-->VlogPtr->+-----------+
      | rotates when       (active   | SSTs      |  reverse scan;
      | oversized at       or        | one per   |  durability +
      | seal time          sealed)   | seal_view |  SstOnly reads
      v                              +-----------+
+-----------+
| Sealed    |
| Vlog Segs | <-- read via VlogPtr
+-----------+

+-----------------------------------------------+
| Value-log entries: bitcode payload + CRC32    |
+-----------------------------------------------+

+-----------------------------------------------+
| Manifest: SST list, vlog segment list,        |
| next_sst_id. Atomic write-fsync-rename        |
| at each seal_view().                          |
+-----------------------------------------------+
```

**Engine design:** tapirs uses a WiscKey-like storage engine for persistent MVCC data. A memtable (`BTreeMap<K, V>` by default, pluggable via the `Memtable<K, V>` trait) holds raw values for the current view. At view change, `seal_view()` serializes the memtable into an append-only value-log segment, records the resulting `(segment_id, offset)` pointers in an in-memory K→`VlogPtr` index, and flushes the index to a per-seal SST on disk. Values live only in the value log; the index and SSTs store only keys + pointers — so re-flushing the index on each seal rewrites small records, not full values. MVCC is built into composite keys: `(K, Timestamp)` allows multiple versions of the same logical key to coexist (see [src/storage/tapir/store.rs](src/storage/tapir/store.rs)).

**Write and read paths:** Writes go only to the memtable (`put()` is a memtable insert). Materialization happens at `seal_view()` — there is no size-based flush trigger; seals are driven by view change. A seal writes the memtable into the active vlog segment, updates the in-memory index, writes one SST containing just that seal's new entries, and clears the memtable. The active vlog segment rotates (moves into `sealed_segments`) when it exceeds `min_vlog_size` at seal time. Read path: memtable → in-memory index (resolve `VlogPtr` → vlog read) → SSTs in reverse order (newest seal first). In `IndexMode::SstOnly`, the in-memory index step is skipped and reads go memtable → SSTs directly. There is no L0/L1 level structure and no background compaction — `sst_readers` is a flat `Vec` appended to on each seal.

**Crash-safe seal checkpoint:** Each `seal_view()` updates the manifest atomically via write-to-tmp → fsync(tmp) → rename → fsync(dir) (see [manifest.rs](../../../src/storage/wisckeylsm/manifest.rs)). If the process crashes mid-seal, the old manifest still lists the pre-seal SST and vlog-segment set, so partial writes are effectively rolled back on recovery. Value-log entries carry a CRC32 checksum verified on every read ([vlog.rs](../../../src/storage/wisckeylsm/vlog.rs)).

**Value-log reclamation:** There is no in-place vlog scanner that rewrites live entries and reclaims dead ones. Reclamation happens at the shard level instead: `tapictl compact shard` (see [cdc.rs::compact](../../../src/sharding/shardmanager/cdc.rs) and [tapictl compact shard](../../operate/cli-tapictl-compact.md)) migrates a shard onto fresh replicas, shipping only committed `(key, value, timestamp)` entries via `IO::Commit` ([ship_changes](../../../src/sharding/shardmanager/cdc.rs#L918)). The new replicas begin with a clean memtable, index, and vlog; the source shard is then decommissioned, retiring its accumulated SSTs and vlog segments as a whole. This reclaims space from aborted prepares, the unbounded IR record, and fragmented segments.

**Why superseded MVCC versions survive compaction:** Compaction does not reduce per-key MVCC cardinality. The source's CDC capture iterates every committed `IO::Commit` across *all* past view transitions ([replica.rs:820-854](../../../src/tapir/replica.rs#L820-L854)) and emits one `Change { key, value, timestamp: commit_ts }` per write. Every change is then replayed on the destination as its own `IO::Commit` at the original `commit_ts`, which lands in [TapirState::commit](../../../src/storage/tapir/store.rs#L302-L337) → [insert_mvcc_entry](../../../src/storage/tapir/store.rs#L206-L215) as `self.mvcc.put(CompositeKey(key, ts), MvccIndexEntry { .. })`. Because the memtable is keyed on `(K, Timestamp)`, each distinct commit timestamp occupies its own entry — if the source held 100 historical versions of key `K`, the destination's memtable ends up with 100 entries, which at the next `seal_view()` become 100 vlog payloads and 100 per-seal SST index entries. Nothing in the ship or seal path merges or collapses superseded versions.

**Extension: bounded history compaction (not implemented).** A practical policy is "preserve full MVCC history at or after timestamp `TS`, and collapse everything older into a single per-key snapshot": `tapictl compact shard --drop-old-versions-before <TS>`. The retain rule per key is (a) every version with `commit_ts >= TS` kept as-is, and (b) the single version with the largest `commit_ts < TS`, which represents the state of that key as of `TS`. A degenerate `TS = 0` reduces to today's behaviour (ship everything); a degenerate `TS = u64::MAX` is "latest-only" (keep exactly one version per key). In the current [compact](../../../src/sharding/shardmanager/cdc.rs#L761) flow the filter runs at the end of phase 3b (drain), on the destination: since the memtable is `BTreeMap<CompositeKey<K, Timestamp>, MvccIndexEntry>`, a single linear scan groups adjacent entries by the `K` component and applies the retain rule. The following `seal_view()` then serializes only the retained entries into the vlog and per-seal SST. Filtering must wait until after drain because phases 1-2 stream deltas lazily and a Change received in phase 1 can be superseded by a Change in phase 2 or 3b — a streaming "keep highest so far" filter is unsafe until the stream is closed.

**Correctness of time travel under bounded history:** Reads with `ts_read >= TS` are unaffected — every version in that range is preserved, so `UO::GetAt`/`UO::ScanAt` and the [timetravel module](../../../src/tapir/timetravel/) resolve exactly as they would on an uncompacted shard. Reads with `ts_read < TS` degrade to "state as of `TS`": any such query returns the single retained pre-`TS` version, not the version that was actually live at `ts_read`. This is semantically equivalent to saying the shard has a retention boundary at `TS` — below it, the database retains committed state but not the change history. Tombstones need no special treatment: a tombstone retained as the pre-`TS` snapshot correctly reports "absent" for all reads in `[its_commit_ts, TS)`, and tombstones with `commit_ts >= TS` are preserved like any other version. The TAPIR compact lifecycle (freeze in phase 3a, drain in phase 3b) already guarantees no in-flight transaction holds a snapshot older than compact-start, so the operator can safely choose `TS` anywhere from "oldest reasonable time-travel window" to `now()`; a typical policy would be `TS = now() - retention_window` (e.g. 7 days). None of this is implemented today — the compact CLI has no `--drop-old-versions-before` flag, and every committed version survives compaction.

**Related docs:** See [Storage concepts](../concepts/storage.md) for term definitions, [Combined store](combined-store.md) for how IR and TAPIR compose five of these engine instances, [Testing](testing.md) for FaultyDiskIo, and [Architecture Decisions](architecture-decisions.md) for why WiscKey over plain LSM. Key files: [src/storage/wisckeylsm/lsm.rs](../../../src/storage/wisckeylsm/lsm.rs), [src/storage/wisckeylsm/vlog.rs](../../../src/storage/wisckeylsm/vlog.rs), [src/storage/io/memtable.rs](../../../src/storage/io/memtable.rs), [src/storage/wisckeylsm/manifest.rs](../../../src/storage/wisckeylsm/manifest.rs).

| Component | Key file | Description |
|-----------|----------|-------------|
| Memtable | `storage/io/memtable.rs` + `Memtable` trait in `storage/wisckeylsm/lsm.rs` | In-memory write buffer for the current view (BTreeMap by default) |
| SST | `storage/wisckeylsm/sst.rs` | Immutable sorted K→VlogPtr index file; one is written per `seal_view()` |
| VlogLsm | `storage/wisckeylsm/lsm.rs` | WiscKey engine: memtable + in-memory K→VlogPtr index + flat Vec of SSTs |
| Value Log | `storage/wisckeylsm/vlog.rs` | Append-only value storage with CRC32 per entry; active + sealed segments |
| Manifest | `storage/wisckeylsm/manifest.rs` | Seal-checkpoint metadata (SST list, vlog-segment list, next_sst_id) |
| DiskIo trait | `storage/io/disk_io.rs` | Pluggable I/O (BufferedIo default, MemoryIo for tests, FaultyDiskIo for fuzz) |

## S3 Remote Storage Layer

```
Write Path (flush)              Read Path (lazy download)
    |                                   |
    v                                   v
+----------+                    +----------+
| DiskIo   | (BufferedIo)       | DiskIo   | (S3CachingIo)
| pwrite() |                    | open()   |
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

**S3CachingIo:** A `DiskIo` implementation that wraps local file I/O with lazy S3 downloads. When `open()` is called and the local file is missing, it downloads the segment from S3 and caches it locally. Subsequent opens use the cached file with zero S3 cost. A process-global registry (keyed by base directory) maps directories to `S3CacheConfig` structs containing bucket, prefix, and endpoint. Used by zero-copy clone and read replicas. Key file: [src/storage/io/s3_caching_io.rs](../../../src/storage/io/s3_caching_io.rs).

**ETag cache invalidation:** On download, S3CachingIo saves a `.etag` sidecar file alongside each segment. On re-open with `expected_size` set (from `VlogSegment::open_at`), if the `.etag` file exists, a HEAD request compares ETags. On mismatch, the segment is fully re-downloaded. Sealed segments are immutable so their ETag never changes. Active segments grow between seals so `sync_to_remote` overwrites them on S3.

**BufferedIo len vs capacity:** `AlignedBuf` over-allocates to alignment boundaries. `BufferedIo.pwrite()` writes `buf.len()` bytes (logical data only). `SyncDirectIo` writes `buf.capacity()` bytes (O_DIRECT requires aligned writes). Files on disk match logical size for BufferedIo.

| Component | Key file | Description |
|-----------|----------|-------------|
| S3CachingIo | `storage/io/s3_caching_io.rs` | Lazy-download DiskIo with ETag cache |
| S3StorageConfig | `storage/remote/config.rs` | S3 bucket/prefix/endpoint/region config |
| sync_to_remote | `storage/remote/sync_to_remote.rs` | Upload new segments + manifest after flush |
