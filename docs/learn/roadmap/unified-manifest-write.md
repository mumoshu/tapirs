# Unified Manifest Write — Eliminate the IR/TAPIR Seal Gap

**Status:** proposed

## Problem

A combined-store flush persists the `UnifiedManifest` **twice**: once after the IR side seals its data files, and again after the TAPIR side seals its data files. See [combined/mod.rs:274-276](../../../src/storage/combined/mod.rs#L274-L276) and [seal_tapir_side](../../../src/storage/combined/mod.rs#L279).

Concretely, `IrReplica::flush()` runs:

1. `record_handle.flush()` → seal `ir_inc` + `ir_con` vlog segments and SSTs → **manifest.save()** (ir_inc / ir_con fields updated, others unchanged).
2. `tapir_handle.flush()` → `seal_tapir_side` → seal `committed` / `prepared` / `mvcc` data files → copy ir_inc / ir_con fields from the in-memory manifest → **manifest.save()** again (all five fields updated).

Each `manifest.save()` is individually atomic (write-fsync-rename). But the pair is not atomic as a whole, so a crash between step 1's rename and step 2's rename leaves the persisted manifest in an intermediate state: fresh IR pointers, stale TAPIR pointers.

Recovery currently papers over this by reopening from whatever the manifest says and relying on the IR-replica layer to replay IR ops through TAPIR upcalls to re-derive the missing TAPIR state. This works because of the "IR is authoritative, TAPIR-side is derivable" invariant documented in [Combined store](../internals/combined-store.md#3-ir-is-authoritative--tapir-side-state-is-derivable), but it adds a recovery step whose sole purpose is fixing a gap the flush code itself creates.

## Proposal

Collapse the two manifest saves into one at the end of the combined flush.

```
record_handle.flush():     seal ir_inc + ir_con data files
                           update in-memory manifest (ir_inc, ir_con)
                           // no manifest.save()

tapir_handle.flush():      seal committed + prepared + mvcc data files
                           update in-memory manifest (all five + max_read_time + txn_log_count + view)
                           manifest.save()     // the ONLY persisted fsync-rename
```

Optionally (housekeeping, not correctness): a startup sweep in `CombinedStoreInner::open` that removes any `*_sst_NNNN.db` / `*_vlog_NNNN.dat` whose IDs are `>= next_*_id` in the persisted manifest — these are orphan files from a crashed flush.

## Correctness analysis

### Crash scenarios under the proposed design

| When the crash happens | On-disk state | Recovery on reopen |
|---|---|---|
| Before any data file is written | No new files | Manifest unchanged → nothing to do |
| After some IR data files, before TAPIR data files | New IR files exist, manifest doesn't reference them | Pre-flush manifest is authoritative; new IR files are orphans |
| After all data files, before manifest write | All five sides have orphan files | Pre-flush manifest is authoritative; all new files are orphans |
| During the single manifest save | atomic write-fsync-rename; either old or new manifest survives | Either pre- or post-flush state, never in-between |

There is **no** scenario where the persisted manifest references new IR segments while referring to stale TAPIR segments (or vice versa). IR and TAPIR are always on the same side of the crash boundary.

### No dangling references

TAPIR's `committed` and `prepared` VlogLsms hold [`TxnLogRef::Committed { op_id, .. }`](../../../src/storage/combined/mod.rs#L79-L82) and [`PreparedRef { op_id, .. }`](../../../src/storage/combined/mod.rs#L66-L70) — references into `ir_inc` / `ir_con`. In the proposed design, either both sides' new data is referenced by the new manifest (flush succeeded), or neither side's is (crash, both sides' files become orphans). Dangling refs are impossible.

### IR client-visible durability is unchanged

IR does not currently rely on single-replica fsync ordering for client-visible durability — it relies on f+1-replica quorum. Losing the last flush's IR data on one replica is survivable: the other replicas have the ops, and view change recovers any missing entries. Deferring the manifest save on one replica doesn't weaken this.

### The replay mechanism remains — for other reasons

Replay of IR ops through TAPIR upcalls is still needed on reopen for the always-rebuild runtime caches (`occ_cache`, `min_prepare_times`, `record_delta_during_view`) and for view-change recovery from other replicas. This proposal eliminates one specific reason the replay is needed (the seal skew), not replay itself.

## Trade-offs

1. **Orphan data files on crash.** Today's design already leaves orphan IR data files during the window between step 1's data-file writes and step 1's manifest save; the proposed design extends the window to cover step 2 as well. Cleanup is trivial (`next_*_id` is read from the persisted manifest, so new IDs never collide; a startup sweep can delete stale-ID files).
2. **No independent IR-only durability.** If anyone ever decouples IR flush from TAPIR flush (e.g., to flush IR more eagerly), the proposal prevents it. Today nothing does.
3. **One fewer fsync per flush cycle.** Small perf win; latency-sensitive on slow disks.

## Where the change lives

- Remove the `manifest.save::<IO>(&base_dir)` call inside whichever IR flush path currently persists the manifest. Likely [ir/store.rs::seal_current_view](../../../src/storage/ir/store.rs#L98) or a sibling.
- Keep [seal_tapir_side::save](../../../src/storage/combined/mod.rs#L347) as the single persistence point.
- Optional: add a startup orphan sweep in [CombinedStoreInner::open](../../../src/storage/combined/mod.rs#L122).
- Verify in tests: an injected crash between step 1 and step 2 must reopen to the pre-flush manifest, not to an intermediate state. Existing combined-store reopen tests ([combined/mod.rs:843](../../../src/storage/combined/mod.rs#L843), [:947](../../../src/storage/combined/mod.rs#L947)) should be extended.

## Related

- [Combined store](../internals/combined-store.md) — the three orthogonal recovery concerns.
- [Storage internals](../internals/storage.md) — per-VlogLsm seal mechanics.
- [src/storage/combined/mod.rs](../../../src/storage/combined/mod.rs) — flush and reopen paths.
