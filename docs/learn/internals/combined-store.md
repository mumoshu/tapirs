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
- **Seal checkpoint**: all five seal together at view change. Their per-seal SST/vlog metadata is recorded in the single `UnifiedManifest`, which is updated atomically (write-fsync-rename). A crash cannot leave IR and TAPIR in inconsistent on-disk states relative to each other.

## What differs

- **Role**: `ir_inc` / `ir_con` / `mvcc` carry full payloads; `prepared` / `committed` carry references into IR.
- **IndexMode**: `committed` opens with `SstOnly` ([combined/mod.rs:145](../../../src/storage/combined/mod.rs#L145)) — no in-memory K→VlogPtr index; reads go straight to SSTs. The committed log is large and rarely read on the hot path, so the memory saving is worth the extra SST hit. The other four keep an `InMemory` index for latency-sensitive lookups.

## Recovery

Not all replica state is persisted. The following runtime caches are rebuilt on open by replaying IR ops against the reopened TAPIR VlogLsms:

- `occ_cache` (prepared reads/writes, `max_read_time`)
- `min_prepare_times` (per-view barrier)
- `record_delta_during_view` (CDC delta index)

IR is authoritative on open; TAPIR-side references become valid once IR has been reopened. See [combined/mod.rs:31-32](../../../src/storage/combined/mod.rs#L31-L32):

> Each side seals independently; crash gaps recovered by replaying IR ops to rebuild TAPIR state.

## Related docs

- [Storage internals](storage.md) — the `VlogLsm` engine itself.
- [IR concepts](../concepts/ir.md) — what an IR record *is* and why it accumulates.
- [TAPIR concepts](../concepts/tapir.md) — prepare/commit lifecycle that produces these entries.
- [tapictl compact shard](../../operate/cli-tapictl-compact.md) — how the accumulated state is reclaimed.
