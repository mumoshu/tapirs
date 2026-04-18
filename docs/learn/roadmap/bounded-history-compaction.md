# Bounded-History Compaction

**Status:** proposed

## Problem

`tapictl compact shard` reclaims the IR record, aborted prepares, per-seal SSTs, and fragmented vlog segments by migrating a shard onto fresh replicas. It does **not** reduce per-key MVCC cardinality: the source's CDC capture iterates every committed `IO::Commit` across all past view transitions ([replica.rs:820-854](../../../src/tapir/replica.rs#L820-L854)) and ships each as its own `IO::Commit` at its original `commit_ts`, which lands in the destination's memtable as a distinct `(CompositeKey<K, TS>, MvccIndexEntry)` pair ([store.rs:206-215](../../../src/storage/tapir/store.rs#L206-L215)). If the source had 1000 historical versions of key `K`, the destination ends up with 1000 entries.

For workloads where only recent history is ever read back (via `UO::GetAt` / `UO::ScanAt` / the [timetravel module](../../../src/tapir/timetravel/)), this is pure waste — old versions are re-materialized on the destination purely to be queried "what happened at ts=3 years ago" on rare occasions, if ever.

## Proposal

Add a retention watermark flag: `tapictl compact shard --drop-old-versions-before <TS>`.

**Retain rule (per key):**

- Every version with `commit_ts >= TS` is kept as-is.
- Plus the single version with the largest `commit_ts < TS` — this represents the visible state of the key as of `TS`.

Degenerate cases:

- `TS = 0` → today's behaviour (ship everything, drop nothing).
- `TS = u64::MAX` → "latest only" (keep exactly one version per key).

A typical production policy: `TS = now() - retention_window` (e.g. 7 days).

## Implementation sketch

The filter runs at the end of phase 3b (drain), on the compact coordinator, before the final `ship_changes` wave. Because the memtable on the destination is keyed by `CompositeKey<K, Timestamp>`, the filter can also be applied destination-side as a single linear scan after shipping completes.

### Option A: coordinator-side filter (preferred)

After drain finishes and the final delta set is available, group the `Vec<Change>` by the `key` field, and within each group retain (a) all entries with `change.timestamp >= TS` and (b) the single entry with the largest `change.timestamp` among those `< TS`. Pass the filtered list to [ship_changes](../../../src/sharding/shardmanager/cdc.rs#L918). Dropped versions never touch destination bandwidth, memtable, vlog, or SSTs.

### Option B: destination-side collapse

Let shipping proceed unchanged. At the end of phase 3b, invoke a new `CombinedStoreInner::collapse_pre_TS(ts: Timestamp)` method that linearly scans the memtable, applies the retain rule, and rewrites it. The following `seal_view()` then serializes only the retained entries.

Option A is cheaper (saves bandwidth); Option B is simpler to retrofit (no change to the CDC path). Option A is the recommended default.

### Why the filter cannot stream during phases 1-2

Phases 1 and 2 stream deltas lazily from the source. A Change received in phase 1 can be superseded by a newer Change in phase 2 or 3b — a streaming "keep highest seen so far, discard rest" filter is unsafe while the stream is still open. The filter must run after the stream is closed (end of phase 3b).

## Correctness analysis

### Time-travel reads at `ts_read >= TS`

Fully preserved. Every version in `[TS, ∞)` is retained unchanged, so `UO::GetAt` / `UO::ScanAt` resolve exactly as they would on an uncompacted shard.

### Time-travel reads at `ts_read < TS`

Degrade to "state as of `TS`": a query returns the retained pre-`TS` snapshot rather than the version actually live at `ts_read`. Semantically equivalent to saying the shard has a retention boundary at `TS` — below it, only committed state is preserved, not change history.

### Tombstones

Need no special treatment:

- A retained pre-`TS` tombstone correctly reports "absent" for all reads in `[its_commit_ts, TS)`.
- A tombstone with `commit_ts >= TS` is preserved like any other post-TS version.

### In-flight transactions

The existing compact lifecycle already handles this: phase 3a freezes new prepares, phase 3b drains pending ones. After drain, no in-flight transaction holds a snapshot older than compact-start, so the operator can choose any `TS` up to `now()` without risking a reader seeing a missing version mid-transaction.

### OCC state transfer

`max_read_time` and other OCC watermarks are transferred via the compact lifecycle (they live in the manifest / OCC cache, not per-key). Unaffected by the filter.

## Trade-offs

1. **Time-travel below `TS` is lost.** This is the entire point of the flag; operators opt in explicitly.
2. **Operator must pick `TS`.** A default policy (e.g. "7 days") is reasonable; the CLI could compute `now() - 7d` if the flag is supplied without an argument.
3. **Filter runs at end of phase 3b** → compact duration grows by the filter pass. For Option A this is O(N changes); for Option B it's O(memtable size). Both are linear, single-pass, in-memory.
4. **No rollback.** Once a shard is compacted with `--drop-old-versions-before TS`, the old versions are gone. Operator should take a backup first if they're not sure.

## Where the change lives

- New flag on the compact CLI: [src/bin/tapictl/main.rs](../../../src/bin/tapictl/main.rs).
- New parameter on the HTTP endpoint: [shardmanager_server/handle_request.rs::/v1/compact](../../../src/sharding/shardmanager_server/handle_request.rs).
- New parameter on [ShardManager::compact](../../../src/sharding/shardmanager/cdc.rs#L761).
- Filter applied before the final [ship_changes](../../../src/sharding/shardmanager/cdc.rs#L918) call (Option A) or in a new collapse method on `CombinedStoreInner` (Option B).
- Operator doc: add a section to [cli-tapictl-compact.md](../../operate/cli-tapictl-compact.md) once implemented.

## Related

- [Storage internals](../internals/storage.md) — value-log reclamation section.
- [tapictl compact shard](../../operate/cli-tapictl-compact.md) — today's compact flow.
- [Combined store](../internals/combined-store.md) — how the 5 VlogLsms relate.
