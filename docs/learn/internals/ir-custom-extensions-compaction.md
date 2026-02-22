# Record Compaction

```
IR Record Growth (beyond the paper)
+----------------------------------------------------------+
|                                                          |
|  Why the IR record grows unboundedly                     |
|                                                          |
|  Every transaction leaves 2+ entries:                    |
|    CO::Prepare{T}  (consensus op, carries write set)     |
|    IO::Commit{T}   (inconsistent op, ID + timestamp)    |
|    or IO::Abort{T}                                       |
|                                                          |
|  In-place compaction of CO::Prepare is UNSAFE because:  |
|    1. CO::Prepare is the sole carrier of the write set  |
|    2. IO::Commit does NOT carry write data              |
|    3. Lagging replicas need the write set in sync()     |
|    4. IO::Abort does not determine transaction fate     |
|       (backup coordinator may send IO::Commit too)      |
|                                                          |
|  Only safe compaction: shard replacement via CDC         |
|  +----------------------------------------------------+ |
|  | Start fresh shard via CDC -- no IR history needed  | |
|  | Removes all resolved entries, transaction_log, etc.| |
|  +----------------------------------------------------+ |
|                                                          |
+----------------------------------------------------------+
```

**The problem:** The IR record is an unordered set of operations that accumulates forever. Every `CO::Prepare`, `IO::Commit`, and `IO::Abort` stays in the record even after a transaction is fully resolved. For a shard with 10K resolved transactions and 1K pending, the record holds ~21K entries (10K Prepare + 10K Commit/Abort + 1K pending). Per-replica compaction is unsafe because IR replicas are inconsistent -- a single replica's `transaction_log` is its local view, not quorum-agreed truth. Only the coordinator's merged record (built from f+1 DoViewChange payloads) represents quorum agreement.

**Why in-place compaction of CO::Prepare is unsafe:** Two independent reasons prevent removing `CO::Prepare` from the merged record, even when a matching `IO::Commit` or `IO::Abort` is finalized:

1. **Write set loss.** `CO::Prepare` carries the `SharedTransaction` (the full write set). `IO::Commit` carries only `transaction_id` and `timestamp`. A lagging replica that missed the original Prepare and receives a compacted record via StartView cannot apply the transaction's writes during `sync()` -- the write set is gone, creating a permanent MVCC inconsistency fixable only by shard replacement. Without compaction, `sync()` processes `CO::Prepare` (adds to prepared with write set) then `IO::Commit` (finds write set in prepared, applies to MVCC). With compaction, `sync()` only sees `IO::Commit` and has no write set to apply.

2. **Ambiguous transaction fate.** A single replica can have *both* `IO::Abort` and `IO::Commit` for the same transaction. Scenario: primary coordinator sends `IO::Abort` (reaches < f+1), backup coordinator recovers the transaction and sends `IO::Commit` (reaches f+1, including replicas with `IO::Abort`). The merged record R contains both. Even an "abort-only" compaction variant is wrong because the presence of `IO::Abort` does not determine the outcome -- `IO::Commit` from a backup coordinator may coexist.

```
Primary coordinator                    Backup coordinator
  |                                      |
  |-- IO::Abort{T} --> R0 (< f+1)       |
  |   (client crashed)                   |
  |                                      |-- recovers T, all shards prepared
  |                                      |-- IO::Commit{T} --> R0,R1,R2 (f+1)
  |                                      |
  View change: merged R has BOTH IO::Abort{T} AND IO::Commit{T}
  merge() determines: COMMITTED (commit wins)
  If CO::Prepare{T} was compacted based on IO::Abort: write set lost
```

**Why per-replica compaction also fails:**

```
R0 has: transaction_log[T1] = committed  (received IO::Commit)
R1 has: no transaction_log entry for T1  (IO::Commit was fire-and-forget, missed)
        prepared[T1] = finalized          (still waiting for commit)

If R0 compacts Prepare{T1} based on its own transaction_log,
and R0+R1 participate in the next view change:
  Merged record R has no Prepare{T1} from R0 (compacted away)
  R1's sync() won't find the Prepare to reconcile
  → stale prepared entry, inconsistent state
```

**Shard replacement (the only safe compaction):** For compaction (removing all resolved entries, transaction_log, ancient MVCC versions), the CDC-based shard replacement in `shard_manager_cdc.rs` starts a fresh shard with only committed state -- no IR record history needed. This is the only safe mechanism because it rebuilds from scratch rather than selectively removing entries from a live record.

**Related docs:** Back to [IR Custom Extensions](ir-custom-extensions.md). See [Protocol](protocol-tapir.md) for the base view change flow, [Resharding](resharding.md) for CDC-based shard replacement, and [IR concepts](../concepts/ir.md) for record terminology. Key files: `src/tapir/shard_manager_cdc.rs` (shard replacement).

| Topic | Summary | Key file |
|-------|---------|----------|
| Why CO::Prepare can't be compacted in-place | Write set loss + ambiguous transaction fate | (this doc) |
| Why per-replica compaction fails | Single replica's view is not quorum-agreed truth | (this doc) |
| Shard replacement (safe compaction) | CDC-based fresh shard, no IR history | `tapir/shard_manager_cdc.rs` |
