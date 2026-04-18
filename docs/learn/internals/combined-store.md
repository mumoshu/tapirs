# Combined Store: IR + TAPIR on a Shared Engine

Both IR and TAPIR persist their state in [`VlogLsm`](../../../src/storage/wisckeylsm/lsm.rs) — the same engine described in [Storage internals](storage.md). A shard replica opens **five** parallel `VlogLsm` instances in a single directory under one [`UnifiedManifest`](../../../src/storage/wisckeylsm/manifest.rs#L36), and the TAPIR-side instances hold small *references* into the IR-side instances rather than duplicating transaction payloads. See [src/storage/combined/](../../../src/storage/combined/).

## The five VlogLsms

| Name | Owner | Key | Value | IndexMode | Holds |
|------|-------|-----|-------|-----------|-------|
| `ir_inc` | IR | `OpId` | `InconsistentEntry<IO>` | `InMemory` | `IO::Commit` / `IO::Abort` payloads |
| `ir_con` | IR | `OpId` | `ConsensusEntry<CO, CR>` | `InMemory` | `CO::Prepare` payload + chosen result |
| `mvcc` | TAPIR | `CompositeKey<K, Timestamp>` | `MvccIndexEntry` | `InMemory` | per-`(K, TS)` version index |
| `prepared` | TAPIR | `TransactionId` | `Option<PreparedRef>` | `InMemory` | `op_id` → `ir_con` (plus `commit_ts`, `finalized`) |
| `committed` | TAPIR | `TransactionId` | `TxnLogRef` | `SstOnly` | `op_id` → `ir_inc` (Committed) or commit TS only (Aborted) |

## Reference edges (no data duplication)

```
TAPIR.prepared.get(txn_id)   ─► PreparedRef { op_id, .. }
                                       │
                                       ▼
                                IR.ir_con.get(op_id)  ─► CO::Prepare { transaction, .. }

TAPIR.committed.get(txn_id)  ─► TxnLogRef::Committed { op_id, .. }
                                       │
                                       ▼
                                IR.ir_inc.get(op_id)  ─► IO::Commit { transaction, .. }
```

The full cross-shard transaction payload lives **once**, in IR; TAPIR resolves it lazily through `op_id`. See [combined/mod.rs:8-18](../../../src/storage/combined/mod.rs#L8-L18) for the motivating "data stored twice" problem this solves.

## What's the same across all five

- **Engine**: `VlogLsm` — a memtable (current view) plus an optional in-memory K→VlogPtr index plus a flat `Vec<SSTableReader>` (one SST per `seal_view()`).
- **Write path**: `put()` is a memtable insert; no disk I/O until seal.
- **Read path**: memtable → in-memory index (if any) → SSTs reverse-scan.
- **Seal format**: every seal event appends per-seal SST + vlog metadata into the single `UnifiedManifest`. Each manifest write is atomic on its own (write-fsync-rename of the whole file).

Seal *timing*, however, is not uniform across the five — see below.

## What differs

- **Role**: `ir_inc` / `ir_con` / `mvcc` carry full payloads; `prepared` / `committed` carry references into IR.
- **IndexMode**: `committed` opens with `SstOnly` ([combined/mod.rs:145](../../../src/storage/combined/mod.rs#L145)) — no in-memory K→VlogPtr index; reads go straight to SSTs. The committed log is large and rarely read on the hot path, so the memory saving is worth the extra SST hit. The other four keep an `InMemory` index for latency-sensitive lookups.

## Three orthogonal recovery concerns

The module comment at [combined/mod.rs:31-32](../../../src/storage/combined/mod.rs#L31-L32) — *"Each side seals independently; crash gaps recovered by replaying IR ops to rebuild TAPIR state"* — is compressing three separate properties that are worth pulling apart:

### 1. Sequential seal and the crash gap

IR and TAPIR seal *sequentially*, not atomically together. An `IrReplica::flush()` cycle is:

1. `record_handle.flush()` → seals `ir_inc` and `ir_con`, writes the unified manifest with fresh `ir_inc` / `ir_con` fields.
2. Then `tapir_handle.flush()` → [`seal_tapir_side`](../../../src/storage/combined/mod.rs#L279) seals `committed` / `prepared` / `mvcc`, copies the already-updated `ir_inc` / `ir_con` fields from IR's in-memory manifest, and writes the unified manifest **again** with all five updated.

If the process crashes *between* steps 1 and 2, the persisted manifest has fresh IR pointers but stale TAPIR pointers — the on-disk view is inconsistent across the two sides. This is the crash gap.

### 2. Runtime caches are always rebuilt on open (not crash-specific)

Three TAPIR runtime structures have no persisted form and are reinitialized to empty on every reopen — crash or clean shutdown alike:

- `occ_cache` — prepared reads/writes, `max_read_time` watermark ([combined/mod.rs:170](../../../src/storage/combined/mod.rs#L170), [:243](../../../src/storage/combined/mod.rs#L243))
- `min_prepare_times` — per-view prepare barrier
- `record_delta_during_view` — CDC delta index

These are reconstructed from the persisted MVCC/prepared/committed VlogLsms plus any replayed IR ops. This concern applies even when steps 1 and 2 above both succeeded — it's about what the manifest *doesn't* store, not about crash consistency.

### 3. IR is authoritative — TAPIR-side state is derivable

The design property that makes both 1 and 2 tractable: TAPIR's `prepared` / `committed` / `mvcc` VlogLsms carry no information that isn't also in the IR record. On reopen, the IR replica layer replays inconsistent and consensus operations through the TAPIR upcalls, which re-derive the TAPIR-side state from the IR payloads. A persisted TAPIR view that's behind the IR view can always be caught up by replay; the reverse (TAPIR ahead of IR) is not possible because TAPIR entries only ever reference IR `op_id`s that already exist.

### How the three interact

- **No crash** → property 2 still applies (runtime caches empty on reopen), properties 1 and 3 are inert.
- **Crash during step 1** → TAPIR-side manifest is untouched; IR-side may have a partially-written manifest rejected by the atomic write-fsync-rename. Nothing to reconcile beyond property 2.
- **Crash between step 1 and step 2** → property 1's gap exists; property 3 says it's recoverable; the IR-replica replay layer performs the recovery; property 2 applies on top.

## Related docs

- [Storage internals](storage.md) — the `VlogLsm` engine itself.
- [IR concepts](../concepts/ir.md) — what an IR record *is* and why it accumulates.
- [TAPIR concepts](../concepts/tapir.md) — prepare/commit lifecycle that produces these entries.
- [tapictl compact shard](../../operate/cli-tapictl-compact.md) — how the accumulated state is reclaimed.
- [Roadmap: unified manifest write](../roadmap/unified-manifest-write.md) — a proposal to eliminate the seal-gap described above by collapsing the two manifest writes into one.
- [Roadmap: segment-install startup and recovery](../roadmap/segment-install-startup-recovery.md) — a proposal to eliminate per-op replay in shard-compaction destinations, backup restore, and OccCache rebuild by reusing the view-change segment-install pattern.
