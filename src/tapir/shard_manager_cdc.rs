use super::replica::{ShardConfig, ShardPhase};
use super::{Change, Key, KeyRange, ShardNumber, Value};
use crate::discovery::ShardDirectory as AddressDirectory;
use crate::tapir::shard_manager::ShardManager;
use crate::tapir::{Replica, Sharded};
use crate::transport::Transport;
use crate::{IrClientId, IrMembership, OccTransaction, OccTransactionId};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug)]
pub enum ReshardError {
    ShardNotRegistered(ShardNumber),
    RangesNotAdjacent(ShardNumber, ShardNumber),
}

/// CDC scan cursor that tracks the highest consumed `effective_end_view`.
///
/// `effective_end_view` from `ScanChangesResult` is `Option<u64>`:
/// - `None` → no CDC deltas exist (no view changes have happened yet).
/// - `Some(N)` → deltas up through base_view=N are available.
///
/// The cursor mirrors this with its own `Option<u64>`:
/// - `None` → no deltas consumed yet → `next_from()` returns 0.
/// - `Some(N)` → consumed through base_view=N → `next_from()` returns N+1.
///
/// **Stabilization**: The cursor is "stabilized" when `last_view` matches
/// `effective_end_view` — meaning we've consumed everything the source has.
/// The loop breaks when stabilized AND no new changes arrived.
struct CdcCursor {
    last_view: Option<u64>,
}

impl CdcCursor {
    fn new() -> Self {
        Self { last_view: None }
    }

    /// The `from_view` argument for the next `scan_changes` call.
    fn next_from(&self) -> u64 {
        match self.last_view {
            Some(v) => v + 1,
            None => 0,
        }
    }

    /// Advance the cursor from a scan result's `effective_end_view`.
    /// Only advances when the source reported deltas exist (`Some`).
    fn advance(&mut self, effective_end_view: Option<u64>) {
        if let Some(eev) = effective_end_view {
            self.last_view = Some(match self.last_view {
                Some(prev) => prev.max(eev),
                None => eev,
            });
        }
    }

    /// True when we've consumed everything the source has.
    /// Both `None` (no history) and `Some(N) == Some(N)` count as stable.
    fn stabilized(&self, effective_end_view: Option<u64>) -> bool {
        self.last_view == effective_end_view
    }
}

impl<K: Key + Clone, V: Value + Clone, T: Transport<Replica<K, V>>, D: AddressDirectory<T::Address>> ShardManager<K, V, T, D> {
    /// Split a shard at `split_key`: keys < split_key stay on `source`,
    /// keys >= split_key move to `new_shard`.
    pub async fn split(
        &mut self,
        source: ShardNumber,
        split_key: K,
        new_shard: ShardNumber,
        new_membership: IrMembership<T::Address>,
    ) -> Result<(), ReshardError> {
        let source_range = self.shards.get(&source)
            .ok_or(ReshardError::ShardNotRegistered(source))?
            .key_range
            .clone();

        let new_range = KeyRange {
            start: Some(split_key.clone()),
            end: source_range.end.clone(),
        };
        let narrowed_range = KeyRange {
            start: source_range.start.clone(),
            end: Some(split_key.clone()),
        };

        // Update source's key range in our map before registering the new shard
        // to avoid overlapping ranges in the directory.
        if let Some(managed) = self.shards.get_mut(&source) {
            managed.key_range = narrowed_range.clone();
        }
        self.register_shard(new_shard, new_membership, new_range.clone());

        let mut cursor = CdcCursor::new();

        // Phase 1: Bulk copy via scan_changes.
        let r = self.shards[&source].client.scan_changes(0).await;
        let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
        let filtered = filter_changes(&changes, &split_key);
        ship_changes(&self.shards[&new_shard].client, new_shard, &filtered, &mut self.rng).await;
        cursor.advance(r.effective_end_view);

        // Phase 2: Catch-up tailing.
        loop {
            let r = self.shards[&source]
                .client
                .scan_changes(cursor.next_from())
                .await;
            let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
            let filtered = filter_changes(&changes, &split_key);
            if !filtered.is_empty() {
                ship_changes(&self.shards[&new_shard].client, new_shard, &filtered, &mut self.rng).await;
            }
            if changes.is_empty() && cursor.stabilized(r.effective_end_view) {
                break;
            }
            cursor.advance(r.effective_end_view);
        }

        // Phase 3a: Freeze source — reject all Prepare with Fail.
        let freeze = serde_json::to_vec(&ShardConfig::<K> {
            key_range: None,
            phase: ShardPhase::ReadOnly,
        })
        .expect("serialize freeze config");
        self.shards[&source].client.reconfigure(freeze);
        // Source frozen — drain pending prepares.

        // Phase 3b: Drain — wait for ALL prepared transactions to resolve + final seal.
        //
        // After freeze: no new CO::Prepare accepted (phase=ReadOnly).
        // Existing prepares resolve via tick() -> recover_coordination().
        // IO::Commit for each resolution is captured in the current view.
        // Need one more view change after all prepares resolve to seal final commits.
        let narrowed_range = KeyRange {
            start: source_range.start.clone(),
            end: Some(split_key.clone()),
        };
        loop {
            T::sleep(Duration::from_secs(1)).await;
            let r = self.shards[&source]
                .client
                .scan_changes(cursor.next_from())
                .await;
            let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
            let filtered = filter_changes(&changes, &split_key);
            if !filtered.is_empty() {
                ship_changes(&self.shards[&new_shard].client, new_shard, &filtered, &mut self.rng).await;
            }
            cursor.advance(r.effective_end_view);

            if r.pending_prepares == 0 && changes.is_empty() && cursor.stabilized(r.effective_end_view) {
                break;
            }
        }
        // One final poll after all prepares resolved to capture sealed commits.
        T::sleep(Duration::from_secs(3)).await;
        let r = self.shards[&source]
            .client
            .scan_changes(cursor.next_from())
            .await;
        let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
        let filtered = filter_changes(&changes, &split_key);
        if !filtered.is_empty() {
            ship_changes(&self.shards[&new_shard].client, new_shard, &filtered, &mut self.rng).await;
        }

        // Phase 3c: Unfreeze source and narrow its key range.
        let unfreeze = serde_json::to_vec(&ShardConfig {
            key_range: Some(narrowed_range.clone()),
            phase: ShardPhase::ReadWrite,
        })
        .expect("serialize unfreeze config");
        self.shards[&source].client.reconfigure(unfreeze);

        // Reconfigure the new shard with its key range.
        let new_config = serde_json::to_vec(&new_range).expect("serialize key range");
        self.shards[&new_shard].client.reconfigure(new_config);

        // Update the source shard's key range in our registry.
        if let Some(managed) = self.shards.get_mut(&source) {
            managed.key_range = narrowed_range.clone();
        }

        // Rebuild directory from all registered shards (not just source + new).
        self.rebuild_directory();
        // Split complete.
        Ok(())
    }
    /// Merge two adjacent shards: `absorbed` is removed, its data shipped to `surviving`.
    ///
    /// # Algorithm (mirrors split's 3-phase approach)
    ///
    /// **Validation**: Both shards must exist with adjacent key ranges. The merged
    /// range is computed via `KeyRange::union()`.
    ///
    /// **Phase 1 — Bulk Copy**: `scan_changes(0)` on the absorbed shard, ship ALL
    /// changes to the surviving shard (no key filtering — all data moves).
    ///
    /// **Phase 2 — Catch-up Tailing**: Loop `scan_changes(cursor.next_from())` on the
    /// absorbed shard. Ship new changes to the surviving shard. Stop when the cursor
    /// stabilizes.
    ///
    /// **Phase 3a — Freeze Absorbed**: `reconfigure(ShardConfig { phase: ReadOnly })`.
    /// New Prepare returns Fail. Existing prepared transactions resolve via
    /// `recover_coordination()` tick.
    ///
    /// **Phase 3b — Drain Prepared on Absorbed**: Wait for `pending_prepares == 0`
    /// (using the `any(pp == 0)` check — by quorum intersection, a replica with
    /// pp=0 is authoritative). Continue shipping remaining changes. Final 3-second
    /// sleep + one more poll to capture sealed commits.
    ///
    /// **Phase 3c — Expand Surviving + Cleanup**:
    /// 1. `reconfigure(ShardConfig { key_range: merged_range })` on surviving
    /// 2. Sleep 5 seconds for view change to propagate
    /// 3. Update surviving shard's `key_range` in `self.shards`
    /// 4. `deregister_shard(absorbed)` — removes from shards, address_directory,
    ///    rebuilds directory
    ///
    /// # Key design decisions
    ///
    /// - **No key filtering**: Unlike split which filters `key >= split_key`, merge
    ///   ships ALL changes from the absorbed shard.
    /// - **Absorbed shard is NOT unfrozen**: It's being removed entirely. The freeze
    ///   is permanent.
    /// - **Surviving shard is NOT frozen**: Continues operating normally. Safe because
    ///   key ranges are disjoint — the surviving shard's OCC validates keys in its
    ///   range only, shipped commits write keys in the absorbed range only, and no
    ///   client routes absorbed-range keys to the surviving shard until after
    ///   expansion.
    /// - **IO::Commit has no key_range check**: The surviving shard accepts shipped
    ///   commits for keys outside its current range. So we can ship data throughout
    ///   phases 1–3b before expanding the range in 3c.
    ///
    /// # Downtime analysis
    ///
    /// `ShardPhase::ReadOnly` is ONLY checked in `exec_consensus` for `CO::Prepare`. All
    /// unlogged and inconsistent operations (`UO::Get`, `UO::Scan`,
    /// `IO::QuorumRead`, `IO::QuorumScan`, `IO::Commit`, `IO::Abort`) do NOT
    /// check `phase`.
    ///
    /// | Phase | RW txns (absorbed range) | RO txns (absorbed range) | All txns (surviving range) |
    /// |-------|--------------------------|--------------------------|----------------------------|
    /// | Phase 1-2 (bulk copy + tailing) | Normal | Normal | Normal |
    /// | Phase 3a-3b (freeze + drain) | DOWN — Prepare returns Fail | Normal — reads work | Normal |
    /// | Phase 3c reconfigure surviving | DOWN — Prepare returns Fail | Normal — reads work | Brief disruption (~2-5s view change) |
    /// | Phase 3c deregister absorbed | DOWN — connection errors | DOWN — connection errors | Normal (expanded range) |
    /// | After directory refresh | Normal (routed to surviving) | Normal (routed to surviving) | Normal |
    ///
    /// # CDC cursor and `pending_prepares` semantics
    ///
    /// **Cursor advancement**: Uses `CdcCursor` which tracks `Option<u64>` to
    /// distinguish "no deltas seen" (None) from "latest delta at base_view=0"
    /// (Some(0)). When no deltas have been seen, `next_from()` returns 0; after
    /// seeing deltas at base_view=N, returns N+1. This prevents the drain from
    /// skipping a delta at base_view=0 when no view changes occurred before the
    /// freeze.
    ///
    /// **`pending_prepares` scope**: `self.inner.prepared.len()` — count of ALL
    /// entries in the OCC store's prepared HashMap. View-agnostic. Entries added on
    /// Prepare, removed on Commit/Abort.
    ///
    /// **"Last mile" commits**: When `pending_prepares` reaches 0, resolved
    /// transactions' IO::Commit/Abort are in the IR record but no delta exists yet
    /// (requires view change). The 3-second sleep covers this: IR tick fires every
    /// 2s, first tick skipped by `changed_view_recently`, second tick (~4s after
    /// freeze) triggers a natural view change sealing the commits. The final
    /// `scan_changes` poll picks up this delta.
    pub async fn merge(
        &mut self,
        absorbed: ShardNumber,
        surviving: ShardNumber,
    ) -> Result<(), ReshardError> {
        let absorbed_range = self.shards.get(&absorbed)
            .ok_or(ReshardError::ShardNotRegistered(absorbed))?
            .key_range
            .clone();
        let surviving_range = self.shards.get(&surviving)
            .ok_or(ReshardError::ShardNotRegistered(surviving))?
            .key_range
            .clone();
        if !absorbed_range.adjacent(&surviving_range) {
            return Err(ReshardError::RangesNotAdjacent(absorbed, surviving));
        }
        let merged_range = surviving_range.union(&absorbed_range);

        let mut cursor = CdcCursor::new();

        // Phase 1: Bulk copy — ship all changes from absorbed to surviving.
        let r = self.shards[&absorbed].client.scan_changes(0).await;
        let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
        // Bulk copy complete — ship all changes to surviving shard.
        ship_changes(&self.shards[&surviving].client, surviving, &changes, &mut self.rng).await;
        cursor.advance(r.effective_end_view);

        // Phase 2: Catch-up tailing.
        loop {
            let r = self.shards[&absorbed]
                .client
                .scan_changes(cursor.next_from())
                .await;
            let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
            if !changes.is_empty() {
                ship_changes(&self.shards[&surviving].client, surviving, &changes, &mut self.rng).await;
            }
            if changes.is_empty() && cursor.stabilized(r.effective_end_view) {
                break;
            }
            cursor.advance(r.effective_end_view);
        }

        // Phase 3a: Freeze absorbed — reject all Prepare with Fail.
        let freeze = serde_json::to_vec(&ShardConfig::<K> {
            key_range: None,
            phase: ShardPhase::ReadOnly,
        })
        .expect("serialize freeze config");
        self.shards[&absorbed].client.reconfigure(freeze);
        // Absorbed shard frozen — drain pending prepares.

        // Phase 3b: Drain — wait for pending_prepares == 0 + final seal.
        loop {
            T::sleep(Duration::from_secs(1)).await;
            let r = self.shards[&absorbed]
                .client
                .scan_changes(cursor.next_from())
                .await;
            let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
            if !changes.is_empty() {
                ship_changes(&self.shards[&surviving].client, surviving, &changes, &mut self.rng).await;
            }
            cursor.advance(r.effective_end_view);

            if r.pending_prepares == 0 && changes.is_empty() && cursor.stabilized(r.effective_end_view) {
                break;
            }
        }
        // One final poll after all prepares resolved to capture sealed commits.
        T::sleep(Duration::from_secs(3)).await;
        let r = self.shards[&absorbed]
            .client
            .scan_changes(cursor.next_from())
            .await;
        let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
        if !changes.is_empty() {
            ship_changes(&self.shards[&surviving].client, surviving, &changes, &mut self.rng).await;
        }

        // Phase 3c: Expand surviving shard's key range.
        let expand = serde_json::to_vec(&ShardConfig {
            key_range: Some(merged_range.clone()),
            phase: ShardPhase::ReadWrite,
        })
        .expect("serialize expanded config");
        self.shards[&surviving].client.reconfigure(expand);

        // Wait for view change to propagate.
        T::sleep(Duration::from_secs(5)).await;

        // Update surviving shard's key range in our registry.
        if let Some(managed) = self.shards.get_mut(&surviving) {
            managed.key_range = merged_range;
        }

        // Remove absorbed shard and rebuild directory.
        self.deregister_shard(absorbed);
        // Merge complete.
        Ok(())
    }

    /// Compact a shard: create a fresh replacement shard, migrate all committed
    /// data, resolve all pending transactions, then decommission the original.
    ///
    /// # Motivation
    ///
    /// The IR record stores every operation ever proposed (Prepare,
    /// Commit, Abort, RaiseMinPrepareTime) and grows as O(total_ops). Naive
    /// truncation of Finalized entries is unsafe because new replicas joining
    /// via `add_replica()` → `fetch_leader_record()` → `bootstrap_record()` →
    /// view change reconstruct their full TAPIR state by replaying the record
    /// through `sync()`. Truncating old entries means new replicas would miss
    /// old prepared entries and MVCC writes.
    ///
    /// Compaction sidesteps truncation entirely: a fresh shard starts with an
    /// empty IR record and empty `record_delta_during_view`, and receives only
    /// committed data via CDC, achieving the same memory reclamation without
    /// modifying the IR layer. Both the IR record and CDC delta accumulation
    /// are reset.
    ///
    /// # Algorithm (adapts merge's 3-phase approach)
    ///
    /// **Setup**: Register `new_shard` with the same key range as `source` and
    /// fresh replicas (`new_membership`). The new shard starts with an empty
    /// IR record, empty prepared set, and empty transaction_log.
    ///
    /// **Phase 1 — Bulk Copy**: `scan_changes(0)` on source shard → ship ALL
    /// changes to the new shard as IO::Commit operations (no key filtering —
    /// same key range). Same approach as existing `split()` and `merge()`.
    ///
    /// **Phase 2 — Catch-up Tailing**: Loop `scan_changes(last_view + 1)` on
    /// the source shard. Ship new changes to the new shard. Stop when
    /// `effective_end_view` stabilizes.
    ///
    /// **Phase 3a — Freeze Source**: `reconfigure(ShardConfig { phase: ReadOnly })`.
    /// New `CO::Prepare` returns Fail. This is the critical safety mechanism
    /// for cross-shard transactions: freezing ensures no NEW prepares are
    /// accepted on the source shard, while existing prepared transactions
    /// continue to be resolvable. Without freezing, a cross-shard transaction
    /// that successfully prepared on all participant shards (and therefore MUST
    /// be committed per TAPIR's guarantee) could have its Prepare on this
    /// shard lost if the source were decommissioned before the coordinator
    /// sent IO::Commit.
    ///
    /// **Phase 3b — Drain Prepared on Source**: Wait for `pending_prepares == 0`.
    /// This ensures ALL cross-shard transactions that this shard participates
    /// in are fully resolved (committed or aborted) before decommissioning:
    ///
    /// - **Transactions prepared on all shards** (must commit): The primary
    ///   coordinator (client) or backup coordinator (`recover_coordination()`
    ///   triggered by replica tick) sends IO::Commit. The commit is captured
    ///   by CDC and shipped to the new shard.
    /// - **Transactions prepared on some but not all shards** (must abort):
    ///   The coordinator eventually sends IO::Abort after detecting that not
    ///   all shards prepared successfully. The abort removes the prepared
    ///   entry; no data is shipped (aborted transactions produce no MVCC writes).
    /// - **In-flight prepares at freeze time**: The `ReadOnly` phase causes
    ///   new Prepare to return Fail, but existing prepared entries in the OCC
    ///   store remain. `recover_coordination()` fires via replica tick for
    ///   any stale prepared entries, resolving them via IO::Commit or IO::Abort.
    ///
    /// Continue shipping remaining changes during drain. Final 3-second sleep +
    /// one more poll to capture sealed commits from the last view change.
    ///
    /// **Phase 3c — Swap + Cleanup**:
    /// 1. Update new shard's `key_range` in `self.shards` (already set at registration)
    /// 2. `deregister_shard(source)` — removes from shards, address_directory,
    ///    rebuilds directory to route all traffic to the new shard
    ///
    /// # Key design decisions
    ///
    /// - **Same key range**: Unlike split (narrows source, creates new range)
    ///   or merge (expands surviving range), compact keeps the key range
    ///   identical. The new shard is a drop-in replacement.
    /// - **Source shard is frozen, NOT unfrozen**: It's being decommissioned.
    ///   The freeze is permanent, same as the absorbed shard in merge.
    /// - **New shard is NOT frozen during migration**: It can accept IO::Commit
    ///   for shipped data while simultaneously handling reads (if clients are
    ///   routed to it, which only happens after directory swap). Safe because
    ///   all shipped IO::Commits write data that was already committed on the
    ///   source — no conflict with the empty new shard's OCC state.
    /// - **No key filtering**: All changes from the source are shipped, same
    ///   as merge. The key range is identical so all data belongs.
    /// - **Cross-shard transaction safety**: The freeze + drain sequence
    ///   guarantees that every prepared transaction is resolved before the
    ///   source shard is decommissioned. A transaction prepared on all shards
    ///   MUST be committed (TAPIR guarantee); the drain waits for the
    ///   coordinator to send IO::Commit, which CDC captures and ships to the
    ///   new shard. No transaction is lost or stuck.
    ///
    /// # Downtime analysis
    ///
    /// `ShardPhase::ReadOnly` is ONLY checked in `exec_consensus` for `CO::Prepare`. All
    /// unlogged and inconsistent operations (`UO::Get`, `UO::Scan`,
    /// `IO::QuorumRead`, `IO::QuorumScan`, `IO::Commit`, `IO::Abort`) do NOT
    /// check `phase`.
    ///
    /// | Phase | RW txns (source range) | RO txns (source range) |
    /// |-------|------------------------|------------------------|
    /// | Phase 1-2 (bulk copy + tailing) | Normal | Normal |
    /// | Phase 3a-3b (freeze + drain) | DOWN — Prepare returns Fail | Normal — reads work |
    /// | Phase 3c deregister source | DOWN — connection errors | DOWN — connection errors |
    /// | After directory refresh | Normal (routed to new shard) | Normal (routed to new shard) |
    ///
    /// # CDC cursor and `pending_prepares` semantics
    ///
    /// Same as merge — see `merge()` doc comment for details on cursor
    /// advancement, `pending_prepares` scope, and "last mile" commit semantics.
    ///
    /// # Structures reclaimed
    ///
    /// Compaction resets **all** unbounded per-shard in-memory structures by
    /// replacing the source shard with a fresh one:
    ///
    /// | Structure | Source shard (bloated) | New shard (after compact) |
    /// |-----------|----------------------|--------------------------|
    /// | IR record (`BTreeMap<OpId, Entry>`) | O(total_ops) | Empty (only current-view entries) |
    /// | `record_delta_during_view` | Accumulated deltas from all views | Empty |
    /// | `transaction_log` (`BTreeMap<TxnId, (Timestamp, bool)>`) | O(total_committed + aborted txns) | Empty |
    /// | `prepared` set | Pending prepares | Empty (all drained) |
    /// | `range_reads` (`Vec<(K, K, TS)>`) | O(total_scan_ops) | Empty |
    /// | MVCC data store | All committed key versions | All committed data (shipped via CDC) |
    ///
    /// # `range_reads` safety
    ///
    /// `range_reads: Vec<(K, K, TS)>` in the OCC store tracks range-level
    /// read timestamps from read-only `QuorumScan` operations. Each entry
    /// `(start, end, read_ts)` blocks new Prepares that write keys in
    /// `[start, end]` at `commit_ts < read_ts`. This prevents **phantom
    /// writes** — a new write appearing in a range that was previously
    /// scanned — which would violate serializability of the scan.
    ///
    /// `range_reads` is used **only** during `occ_check()` for `CO::Prepare`
    /// conflict detection. It is NOT used by `transaction_log`, `commit_get`,
    /// CDC, resharding, or any read path.
    ///
    /// `range_reads` grows with every `QuorumScan` operation and is **never
    /// cleaned** — no `retain`, `clear`, or `drain` exists. Over time, this
    /// Vec grows as O(total_scan_operations), unbounded.
    ///
    /// **Why starting with empty `range_reads` is safe:**
    ///
    /// The new shard receives committed data via `IO::Commit` shipping (same
    /// as split and merge). `IO::Commit` writes key-value-timestamp entries
    /// to the MVCC store; it does NOT replay `IO::QuorumScan` entries, so
    /// `range_reads` are not reconstructed. This is the same property that
    /// split and merge already have: the new/surviving shard does not inherit
    /// `range_reads` from the source/absorbed shard.
    ///
    /// After compaction, a Prepare with `commit_ts` below a past scan's
    /// `snapshot_ts` would not be blocked by the (absent) `range_read`.
    /// This is safe because:
    ///
    /// 1. **TAPIR clients pick `commit_ts` close to their wall-clock time.**
    ///    After compaction (which takes non-trivial time for phases 1-3b),
    ///    any new Prepare arrives with a `commit_ts` well above historical
    ///    scan timestamps.
    /// 2. **`min_prepare_time` provides the primary defense.** As the backup
    ///    coordinator resolves transactions, it calls
    ///    `raise_min_prepare_time(commit.time + 1)`, which advances
    ///    `min_prepare_time` on the source shard. While the new shard's
    ///    `min_prepare_time` starts at 0, it advances as new transactions
    ///    commit on it. Prepares with very old timestamps are rejected as
    ///    `TooLate` by the advancing `min_prepare_time`.
    /// 3. **Same property as split/merge.** Existing resharding operations
    ///    already ship data via `IO::Commit` without replaying
    ///    `IO::QuorumScan`. If `range_reads` loss were a correctness issue,
    ///    it would affect split and merge equally — but both are considered
    ///    correct.
    ///
    /// # `transaction_log` safety
    ///
    /// `transaction_log` records whether each transaction was committed or
    /// aborted. It is used **only** for deduplication in `tapir::Replica`:
    ///
    /// - **During Prepare**: If the transaction was already committed, return
    ///   Ok/Retry. If already aborted, return Fail. This prevents a stale
    ///   Prepare from re-entering the `prepared` set for a resolved transaction.
    /// - **During CheckPrepare**: Same deduplication for backup coordinator
    ///   recovery.
    /// - **During IO::Commit/IO::Abort**: `debug_assert`s for duplicate
    ///   detection (debug builds only).
    ///
    /// `transaction_log` is **NOT** used for OCC conflict detection. Conflict
    /// detection uses `prepared_reads`, `prepared_writes`, `range_reads`, and
    /// MVCC store reads (`get_last_read`, `get_last_read_at`,
    /// `has_writes_in_range`) — all in `occ::Store`, which does not reference
    /// `transaction_log` at all.
    ///
    /// **Why starting with an empty `transaction_log` is safe:**
    ///
    /// After compaction, the source shard is frozen, all pending prepares are
    /// drained (committed or aborted by the coordinator system), and the
    /// source is decommissioned. The new shard starts with an empty
    /// `transaction_log`. Without deduplication entries, stale messages for
    /// resolved transactions may cause redundant work, but never produce
    /// incorrect results:
    ///
    /// - **Stale IO::Commit** (for a transaction already committed on the
    ///   source and shipped via CDC): Re-applies MVCC writes. `put(key,
    ///   value, timestamp)` is idempotent — inserting the same
    ///   (key, value, timestamp) into the BTreeMap overwrites with identical
    ///   data. The `transaction_log` entry is re-inserted; harmless.
    /// - **Stale IO::Abort** (for a transaction already resolved on the
    ///   source): Finds nothing in the `prepared` set (new shard has an empty
    ///   prepared set) → `remove_prepared` is a no-op. Harmless.
    /// - **Stale Prepare** (for a transaction already committed on the
    ///   source): The new shard accepts the Prepare (no dedup entry to reject
    ///   it). The coordinator eventually sends IO::Commit, which commits it.
    ///   The MVCC writes are idempotent with the CDC-shipped data — same key,
    ///   same value, same timestamp. Net effect: redundant prepare/commit
    ///   cycle, correct final state.
    /// - **Stale Prepare** (for a transaction already aborted on the source):
    ///   The new shard accepts the Prepare. The coordinator sends IO::Abort,
    ///   which removes it from `prepared`. No data written to MVCC (aborted
    ///   transactions produce no MVCC writes). Harmless.
    ///
    /// In all cases, the coordinator system's IO::Commit or IO::Abort always
    /// arrives to finalize the transaction. The new shard may do redundant
    /// work without `transaction_log` dedup, but the final state is always
    /// correct.
    ///
    /// # Result
    ///
    /// The new shard has all committed data, a clean IR record (only
    /// current-view entries), empty `prepared` set (all transactions resolved
    /// during drain), and empty `transaction_log`. The bloated source shard
    /// with its unbounded IR record, delta accumulation, and transaction log
    /// is removed entirely.
    pub async fn compact(
        &mut self,
        source: ShardNumber,
        new_shard: ShardNumber,
        new_membership: IrMembership<T::Address>,
    ) -> Result<(), ReshardError> {
        let source_range = self.shards.get(&source)
            .ok_or(ReshardError::ShardNotRegistered(source))?
            .key_range
            .clone();

        // Register the new shard with the same key range as the source.
        // Use deferred registration (no directory rebuild) because both source
        // and new shard have the same key range — rebuilding would panic on
        // the overlap check.  deregister_shard(source) at the end rebuilds.
        self.register_shard_deferred(new_shard, new_membership, source_range);

        let mut cursor = CdcCursor::new();

        // Phase 1: Bulk copy — ship all changes from source to new shard.
        let r = self.shards[&source].client.scan_changes(0).await;
        let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
        ship_changes(&self.shards[&new_shard].client, new_shard, &changes, &mut self.rng).await;
        cursor.advance(r.effective_end_view);

        // Phase 2: Catch-up tailing.
        loop {
            let r = self.shards[&source]
                .client
                .scan_changes(cursor.next_from())
                .await;
            let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
            if !changes.is_empty() {
                ship_changes(&self.shards[&new_shard].client, new_shard, &changes, &mut self.rng).await;
            }
            if changes.is_empty() && cursor.stabilized(r.effective_end_view) {
                break;
            }
            cursor.advance(r.effective_end_view);
        }

        // Phase 3a: Freeze source — reject all Prepare with Fail.
        let freeze = serde_json::to_vec(&ShardConfig::<K> {
            key_range: None,
            phase: ShardPhase::ReadOnly,
        })
        .expect("serialize freeze config");
        self.shards[&source].client.reconfigure(freeze);

        // Phase 3b: Drain — wait for pending_prepares == 0 + final seal.
        loop {
            T::sleep(Duration::from_secs(1)).await;
            let r = self.shards[&source]
                .client
                .scan_changes(cursor.next_from())
                .await;
            let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
            if !changes.is_empty() {
                ship_changes(&self.shards[&new_shard].client, new_shard, &changes, &mut self.rng).await;
            }
            cursor.advance(r.effective_end_view);

            if r.pending_prepares == 0 && changes.is_empty() && cursor.stabilized(r.effective_end_view) {
                break;
            }
        }
        // One final poll after all prepares resolved to capture sealed commits.
        T::sleep(Duration::from_secs(3)).await;
        let r = self.shards[&source]
            .client
            .scan_changes(cursor.next_from())
            .await;
        let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
        if !changes.is_empty() {
            ship_changes(&self.shards[&new_shard].client, new_shard, &changes, &mut self.rng).await;
        }

        // Phase 3c: Decommission the source shard.
        self.deregister_shard(source);
        // Compact complete.
        Ok(())
    }
}

/// Filter changes to those with key >= split_key.
fn filter_changes<K: Ord + Clone, V: Clone>(
    changes: &[Change<K, V>],
    split_key: &K,
) -> Vec<Change<K, V>> {
    changes
        .iter()
        .filter(|c| c.key >= *split_key)
        .cloned()
        .collect()
}

/// Ship a set of changes to a target shard by wrapping each as an IO::Commit.
async fn ship_changes<K: Key + Clone, V: Value + Clone, T: Transport<Replica<K, V>>>(
    client: &super::ShardClient<K, V, T>,
    shard: ShardNumber,
    changes: &[Change<K, V>],
    rng: &mut crate::Rng,
) {
    for change in changes {
        let id = OccTransactionId {
            client_id: IrClientId::new(rng),
            number: 0,
        };
        let key = Sharded { shard, key: change.key.clone() };
        let mut write_set = BTreeMap::new();
        write_set.insert(key, change.value.clone());
        let txn = OccTransaction {
            read_set: BTreeMap::new(),
            write_set,
            scan_set: Vec::new(),
        };
        client.commit(id, txn, change.timestamp).await;
    }
}
