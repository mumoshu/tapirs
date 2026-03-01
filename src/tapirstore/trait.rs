use crate::occ::{PrepareConflict, PrepareResult, SharedTransaction, Transaction, TransactionId};
use crate::tapir::{Key, LeaderRecordDelta, ShardNumber, Timestamp, Value};
use serde::{de::DeserializeOwned, Serialize};

/// Result of checking the current status of a transaction for prepare decisions.
#[derive(Debug, PartialEq, Eq)]
pub enum CheckPrepareStatus {
    /// Already committed at the exact requested timestamp.
    CommittedAtTimestamp,
    /// Already committed, but at a different timestamp.
    CommittedDifferent { proposed: u64 },
    /// Already aborted.
    Aborted,
    /// Already prepared at the requested timestamp.
    PreparedAtTimestamp { finalized: bool },
    /// Commit time is before min_prepare_time (too late).
    TooLate,
    /// No decision can be made from local state.
    Unknown,
}

/// Abstracts all stateful operations the TAPIR replica needs.
///
/// Implementations hold the OCC store, transaction log, min-prepare-time
/// tracking, and CDC delta history. The TAPIR replica keeps only transient
/// protocol state (key_range, phase, counters) and delegates all persistent
/// state operations through this trait.
pub trait TapirStore<K: Key, V: Value>: Send + Serialize + DeserializeOwned + 'static {
    // === Identity ===

    fn shard(&self) -> ShardNumber;

    // === Uncommitted Reads ===

    /// Read the latest committed version of `key` (any timestamp).
    ///
    /// **Pure read** -- takes `&self`, no side effects. Does not record a
    /// read timestamp, so this read cannot cause future `try_prepare_txn`
    /// calls to fail OCC checks. Used by the replica for `UO::Get`
    /// (unlogged single-key reads that skip consensus).
    ///
    /// Returns `(value, write_ts)` where `write_ts` is the commit
    /// timestamp of the version returned. If no version exists, returns
    /// `(None, Timestamp::default())`.
    ///
    /// # Return value transitions
    ///
    /// ```text
    /// // After commit_prepared_txn(_, write("x","v1"), ts(1,1)):
    /// do_uncommitted_get("x") => (Some("v1"), ts(1,1))
    ///
    /// // After commit_prepared_txn(_, write("x","v2"), ts(5,1)):
    /// do_uncommitted_get("x") => (Some("v2"), ts(5,1))
    ///
    /// // Key never written:
    /// do_uncommitted_get("y") => (None, Timestamp::default())
    /// ```
    fn do_uncommitted_get(&self, key: &K) -> (Option<V>, Timestamp);

    /// Read the version of `key` visible at snapshot timestamp `ts`.
    ///
    /// **Pure read** -- takes `&self`, no side effects. Returns the latest
    /// committed version with `write_ts <= ts`. Does not record a read
    /// timestamp, so this read cannot block future OCC prepares. Used by
    /// the replica for `UO::GetAt` (unlogged point-in-time reads that skip
    /// consensus).
    ///
    /// Returns `(value, write_ts)`. If no version exists at or before `ts`,
    /// returns `(None, Timestamp::default())`.
    ///
    /// # Valid inputs
    ///
    /// `ts` acts as an upper bound on which versions are visible. Any
    /// `Timestamp` is valid, including `Timestamp::default()` (returns only
    /// versions written at time 0).
    ///
    /// # Return value transitions
    ///
    /// ```text
    /// // After commit_prepared_txn(_, write("x","v1"), ts(1,1))
    /// //   and commit_prepared_txn(_, write("x","v2"), ts(5,1)):
    /// do_uncommitted_get_at("x", ts(3,1))  => (Some("v1"), ts(1,1))
    /// do_uncommitted_get_at("x", ts(10,1)) => (Some("v2"), ts(5,1))
    ///
    /// // After commit_prepared_txn(_, write("x","v3"), ts(7,1)):
    /// do_uncommitted_get_at("x", ts(10,1)) => (Some("v3"), ts(7,1))
    /// do_uncommitted_get_at("x", ts(3,1))  => (Some("v1"), ts(1,1))  // unchanged
    /// ```
    fn do_uncommitted_get_at(&self, key: &K, ts: Timestamp) -> (Option<V>, Timestamp);

    /// Scan all keys in `[start, end]` (inclusive) at snapshot timestamp `ts`.
    ///
    /// **Pure read** -- takes `&self`, no side effects. For each key in
    /// the range, returns the latest committed version with `write_ts <= ts`.
    /// Does not record read timestamps or range protections, so this scan
    /// cannot cause future OCC prepare failures. Used by the replica for
    /// `UO::Scan` (with `ts = Timestamp::MAX`) and `UO::ScanAt`.
    ///
    /// Returns `Vec<(key, value, write_ts)>` sorted by key. Keys with no
    /// version at or before `ts` are omitted. Returns empty `Vec` if no
    /// keys exist in the range.
    ///
    /// # Valid inputs
    ///
    /// `start <= end` in key ordering. Pass `ts` with `time = u64::MAX`
    /// to scan the latest committed versions of all keys.
    ///
    /// # Return value transitions
    ///
    /// ```text
    /// // After commit_prepared_txn(_, write("a","v1"), ts(1,1))
    /// //   and commit_prepared_txn(_, write("b","v2"), ts(1,1)):
    /// do_uncommitted_scan("a", "c", ts(10,1))
    ///   => [("a", Some("v1"), ts(1,1)), ("b", Some("v2"), ts(1,1))]
    ///
    /// // After commit_prepared_txn(_, write("b","v3"), ts(5,1)):
    /// do_uncommitted_scan("a", "c", ts(10,1))
    ///   => [("a", Some("v1"), ts(1,1)), ("b", Some("v3"), ts(5,1))]
    /// do_uncommitted_scan("a", "c", ts(3,1))
    ///   => [("a", Some("v1"), ts(1,1)), ("b", Some("v2"), ts(1,1))]
    /// ```
    fn do_uncommitted_scan(&self, start: &K, end: &K, ts: Timestamp) -> Vec<(K, Option<V>, Timestamp)>;

    // === OCC Prepare/Commit/Abort ===

    fn try_prepare_txn(
        &mut self,
        id: TransactionId,
        txn: SharedTransaction<K, V, Timestamp>,
        commit: Timestamp,
        dry_run: bool,
    ) -> PrepareResult<Timestamp>;

    /// Record a commit in the transaction log and apply the write set.
    /// Panics (debug only) if the transaction was previously logged as aborted
    /// or committed at a different timestamp.
    fn commit_prepared_txn(
        &mut self,
        id: TransactionId,
        txn: &Transaction<K, V, Timestamp>,
        commit: Timestamp,
    );

    fn remove_prepared_txn(&mut self, id: TransactionId) -> bool;

    /// Upsert a prepared transaction with three distinct behaviors:
    ///
    /// - **Insert** (vacant): If the transaction ID doesn't exist in `prepared`, inserts
    ///   the new entry and populates `prepared_reads`/`prepared_writes` caches.
    ///
    /// - **Finalize** (same commit timestamp): If the ID already exists with the same
    ///   commit timestamp, only updates the `finalized` flag. Debug-asserts that the
    ///   transaction content is identical. Caches are untouched.
    ///
    /// - **Replace** (different commit timestamp): If the ID exists but with a different
    ///   commit timestamp, replaces the entry entirely — removes old cache entries and
    ///   adds new ones under the new timestamp.
    ///
    /// Calling this twice with identical arguments is idempotent (hits the finalize path).
    /// Calling with a different commit timestamp for the same ID is not idempotent — it
    /// mutates the caches differently each time.
    fn add_or_replace_or_finalize_prepared_txn(
        &mut self,
        id: TransactionId,
        txn: SharedTransaction<K, V, Timestamp>,
        commit: Timestamp,
        finalized: bool,
    );

    // === Prepared Queries ===

    /// Look up a prepared transaction by ID.
    fn get_prepared_txn(
        &self,
        id: &TransactionId,
    ) -> Option<(&Timestamp, &SharedTransaction<K, V, Timestamp>, bool)>;

    /// Check the current status of a transaction for prepare/check-prepare decisions.
    ///
    /// Performs a cascading lookup: txn_log → prepared_at_timestamp → min_prepare_time,
    /// returning a typed status that callers map to their own result type.
    fn check_prepare_status(&self, id: &TransactionId, commit: &Timestamp) -> CheckPrepareStatus;

    /// Finalize a tentatively prepared transaction after IR quorum confirmation.
    ///
    /// The normal commit path (exec_inconsistent → commit_prepared_txn) never
    /// checks the finalized flag. This method is used in TAPIR merge (view
    /// change), where it finalizes tentative prepared transactions that the
    /// quorum agreed on. Unfinalized entries are discarded by
    /// remove_all_unfinalized_prepared_txns().
    ///
    /// Returns true if the entry existed at the given commit timestamp.
    fn finalize_prepared_txn(&mut self, id: &TransactionId, commit: &Timestamp) -> bool;

    fn prepared_count(&self) -> usize;

    /// Returns the oldest prepared transaction (minimum commit timestamp).
    fn get_oldest_prepared_txn(
        &self,
    ) -> Option<(TransactionId, Timestamp, SharedTransaction<K, V, Timestamp>)>;

    /// Remove all prepared transactions that are NOT finalized.
    fn remove_all_unfinalized_prepared_txns(&mut self);

    // === Committed Read/Scan ===

    fn do_committed_get(
        &mut self,
        key: K,
        ts: Timestamp,
    ) -> Result<(Option<V>, Timestamp), PrepareConflict>;

    fn do_committed_scan(
        &mut self,
        start: K,
        end: K,
        ts: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, PrepareConflict>;

    // === Uncommitted Validated Reads ===

    fn do_uncommitted_get_validated(&self, key: &K, ts: Timestamp) -> Option<(Option<V>, Timestamp)>;
    fn do_uncommitted_scan_validated(
        &self,
        start: &K,
        end: &K,
        ts: Timestamp,
    ) -> Option<Vec<(K, Option<V>, Timestamp)>>;

    // === Transaction Log ===

    fn txn_log_get(&self, id: &TransactionId) -> Option<(Timestamp, bool)>;
    fn txn_log_insert(
        &mut self,
        id: TransactionId,
        ts: Timestamp,
        committed: bool,
    ) -> Option<(Timestamp, bool)>;
    fn txn_log_contains(&self, id: &TransactionId) -> bool;
    fn txn_log_len(&self) -> usize;

    // === Min Prepare Time ===

    /// Raise tentative min_prepare_time to max(current, min(time, min_prepared_timestamp)).
    /// Returns the final min_prepare_time value.
    fn raise_min_prepare_time(&mut self, time: u64) -> u64;

    /// Finalize min_prepare_time: raise finalized_mpt to max(current, time),
    /// then raise tentative mpt to max(current_tentative, new_finalized).
    fn finalize_min_prepare_time(&mut self, time: u64);

    /// Sync min_prepare_time: raise finalized_mpt to max(current, time),
    /// then set tentative mpt to min(current_tentative, new_finalized).
    /// Can rollback tentative prepared time to the finalized value.
    fn sync_min_prepare_time(&mut self, time: u64);

    /// Reset tentative min_prepare_time to the finalized value.
    /// Used during merge to clear out-of-order tentative state.
    fn reset_min_prepare_time_to_finalized(&mut self);

    // === CDC Deltas ===

    fn record_cdc_delta(&mut self, base_view: u64, delta: LeaderRecordDelta<K, V>);
    fn cdc_deltas_from(&self, from_view: u64) -> Vec<LeaderRecordDelta<K, V>>;
    fn cdc_max_view(&self) -> Option<u64>;

    // === Resharding ===

    fn min_prepare_baseline(&self) -> (Option<Timestamp>, Option<Timestamp>);
}
