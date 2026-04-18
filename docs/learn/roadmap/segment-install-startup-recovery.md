# Segment-Install Startup and Recovery — Eliminate Per-Op Replay

**Status:** proposed

## Problem

View change already avoids full replay of historical IR records by using the **segment-install** pattern: sealed SSTs + vlog segments are shipped as bytes between replicas and materialized directly into the storage layer via [install_start_view_payload](../../../src/ir/ir_record_store.rs#L159). A lagging follower catches up in O(bytes shipped) rather than O(N_ops) — see [DoViewChange / StartView analysis](../internals/view-change-doviewchange-always-delta.md).

Several other startup/recovery paths still fall back to per-op replay and therefore scale linearly with the total IR-op history of a shard. In a shard with millions of committed transactions, this produces minutes-to-hours of startup/recovery latency:

| Path | Current behaviour | Cost |
|---|---|---|
| **Shard compaction destination** | [ship_changes](../../../src/sharding/shardmanager/cdc.rs#L918) iterates every committed `Change` from the source's CDC deltas and re-executes each as a full `IO::Commit` RPC through the destination's TAPIR replica, which runs OCC check → prepare → commit → MVCC insert per op. | O(N_committed_ops_in_source) |
| **Backup/snapshot restore** | [restore_from_ir_record_rebuilds_mvcc](../../../src/storage/ir/tests/restore_from_ir_record_only.rs) is the canonical pattern: iterate every `IO::Commit` in the IR record, look up the matching `CO::Prepare`, invoke `commit_transaction_data` per entry. | O(N_ir_entries) |
| **Warm start OccCache rebuild** | [TapirState::open](../../../src/storage/tapir/store.rs#L583) scans `prepared.index_range(..)` and calls `.get()` on every key ever inserted into the `prepared` VlogLsm — including long-finalized tombstones — just to filter out the handful of active prepares. | O(N_prepared_ever_seen) |
| **Crash-gap recovery** (see [Unified manifest write](unified-manifest-write.md)) | Per-op replay of IR entries not yet reflected in the stale TAPIR manifest. | O(gap size) — eliminated if that roadmap lands |

Of these, **shard-compaction destination** is the largest practical offender. A compaction on a shard with 10M committed ops takes hours with per-op RPC; the equivalent byte-level segment install takes seconds.

## Proposal

Extend the segment-install pattern — which already works for view change — to all three remaining scenarios.

### 1. Shard-compaction destination: snapshot-install instead of per-op replay

Replace `ship_changes` (for the shard-compact flow specifically) with a snapshot-install protocol:

1. **Source** runs a local snapshot build after phase 3b (drain) completes: iterate its `mvcc` / `committed` VlogLsms, filter dead entries (optionally applying the [bounded-history-compaction](bounded-history-compaction.md) retention rule), and emit a new set of sealed SSTs + vlog segments to a staging area.
2. **Source** ships those sealed segments (and a fresh `UnifiedManifest`) to each destination replica as bytes.
3. **Destination** materialises them via a new `install_compaction_snapshot` method that reuses the same primitives as `install_start_view_payload` — segments land on disk, manifest is written atomically, no per-op TAPIR path is invoked.

The destination IR record starts empty (as intended by compaction — the whole point is to drop accumulated IR history). Only the TAPIR-derived state (mvcc, committed) is snapshot-installed; `ir_inc` / `ir_con` / `prepared` start fresh.

### 2. Backup restore: snapshot-format backups, not IR-replay backups

Today's `restore_from_ir_record_rebuilds_mvcc` model treats the IR record as the backup unit and reconstructs MVCC via replay. Flip the model:

- **Backup format** = a serialized tarball of the shard's `mvcc` SSTs + vlog segments + manifest snapshot.
- **Restore** = decompress to the replica's base dir + install via the same `install_start_view_payload`-style primitive.

The IR record is not needed for restore because MVCC is the materialized projection of all committed IR ops; restoring MVCC directly short-circuits the replay.

### 3. Warm start OccCache: persist the OccCache snapshot on seal

At each `seal_view()` on the combined store, write a compact `OccCacheSnapshot` into the unified manifest:

```rust
struct OccCacheSnapshot {
    active_prepares: Vec<OccCachedTransaction>,  // bounded by active-prepare count
    max_read_time: Option<u64>,                  // already in manifest today
    min_prepare_times: BTreeMap<ViewNumber, Timestamp>,  // O(views_with_prepares)
}
```

On open, hydrate `OccCache` directly from the snapshot — no scan of the `prepared` VlogLsm. Size of `active_prepares` is bounded by the number of in-flight cross-shard transactions (typically < 100), independent of historical prepare count.

## Complexity reduction

| Scenario | Before | After | Typical factor |
|---|---|---|---|
| Shard compaction destination | O(N_committed_ops) per-op RPCs | O(bytes_shipped) segment install | 100×–1000× (per-op RPC ≈ µs; per-byte ≈ ns) |
| Backup restore (MVCC-only) | O(N_ir_entries) replays | O(bytes_shipped) segment install | similar |
| Warm start OccCache rebuild | O(N_prepared_ever_seen) lookups | O(N_active_prepared) memory-only | 1000×+ in long-running shards |
| Crash-gap recovery | O(gap) per-op replay | 0 (gap does not exist) | eliminated by [unified-manifest-write](unified-manifest-write.md) |

## Correctness analysis

### Byte-level segment transfer is already trusted

`install_start_view_payload` is the proof-of-concept: sealed segments cross the wire, land on the destination's disk, get registered in its manifest, and become queryable without any per-op validation beyond checksum verification. The correctness argument is that a sealed segment is immutable and self-describing (CRC32 per vlog entry at [vlog.rs](../../../src/storage/wisckeylsm/vlog.rs)), so "bytes on source disk" and "bytes on destination disk" are interchangeable. Extending this trust to compaction-destination install and backup restore is a reuse of the same argument, not a new assumption.

### Shard compaction retention semantics

The source must build its snapshot under the same invariants `ship_changes` implicitly enforces today:
- Only entries whose originating transaction is *committed* (not aborted, not stranded in prepare) appear in the snapshot.
- If [bounded-history-compaction](bounded-history-compaction.md) is enabled, the per-key retention rule is applied during the snapshot build, not at read time.
- Aborted prepares and finalized tombstones from the source's `prepared` VlogLsm are never included — the destination's `prepared` starts empty.

`max_read_time`, `min_prepare_times`, and (if enabled) `ghost_filter` are transferred via the manifest, not reconstructed from per-op replay.

### OccCache persistence

`active_prepares` must be written atomically with the sealed `prepared` VlogLsm (same `seal_view` event, same manifest write). Otherwise a crash between the two could persist a snapshot that references a prepared entry not yet visible in the VlogLsm, or vice versa. Piggybacking on the existing unified-manifest seal point keeps this free — the manifest is already the single atomicity unit.

### IR record is still authoritative for cross-replica consistency

None of this changes the IR P1/P2/P3 properties. The IR record on this replica remains the source of truth for `ir_inc` / `ir_con`; segment install only shortcuts the *derivation* of TAPIR-side state from it. A replica that is out of sync with the IR quorum still recovers via view change, using delta-install as today.

## Trade-offs

1. **Larger snapshots than "just the deltas".** A snapshot-install for shard compaction ships the full filtered MVCC segments, which is larger than only the new committed changes since last compaction. Net win because per-byte cost dominates per-op cost at scale, but for small compactions the break-even point might favour per-op.
2. **Snapshot-build pause on source.** Building a consistent snapshot requires a momentary freeze of seal activity on the source (phase 3b already freezes new prepares; the snapshot build slots in there). Duration is O(seal-time): usually sub-second.
3. **Backup format change.** Existing IR-record-based backups become incompatible; a converter may be needed, or a period of dual-format support.
4. **OccCache snapshot size in the manifest.** Adds O(N_active_prepared × ~200 bytes) per seal to the manifest. For N_active_prepared ≤ 100 this is ≤ 20KB — negligible next to the segment metadata already in the manifest.

## Where the change lives

- **New**: `storage/remote/install_snapshot.rs` (or similar) — orchestrates byte-level ship + install for shard compaction. Reuses `install_start_view_payload` primitives.
- **Modified**: [cdc.rs::compact](../../../src/sharding/shardmanager/cdc.rs#L761) — replace the `ship_changes` call in its final phase with `install_compaction_snapshot`. Earlier CDC phases (catch-up during source live operation) can remain per-op.
- **Modified**: [UnifiedManifest](../../../src/storage/wisckeylsm/manifest.rs#L36) — add `occ_cache_snapshot: Option<OccCacheSnapshot>`.
- **Modified**: [TapirState::open](../../../src/storage/tapir/store.rs#L583) and [CombinedStoreInner::open](../../../src/storage/combined/mod.rs#L122) — hydrate OccCache from the snapshot when present; fall back to the current scan for older manifests.
- **New tests**: cross-replica snapshot install for the shard-compact path; manifest-round-trip tests for `OccCacheSnapshot`; backup/restore using the new tarball format.

## Related

- [View change always-delta install](../internals/view-change-doviewchange-always-delta.md) — the existing segment-install pattern this proposal generalises.
- [Unified manifest write](unified-manifest-write.md) — eliminates the crash-gap replay scenario.
- [Bounded-history compaction](bounded-history-compaction.md) — synergistic: a snapshot-install compaction naturally applies the bounded-history filter during snapshot build.
- [Combined store](../internals/combined-store.md) — describes the five VlogLsms and the IR-is-authoritative invariant that makes segment install safe.
- [tapictl compact shard](../../operate/cli-tapictl-compact.md) — the operator-facing command whose cost this proposal drops dramatically.
