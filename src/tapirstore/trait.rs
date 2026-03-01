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

    // === MVCC Reads ===

    fn get(&self, key: &K) -> (Option<V>, Timestamp);
    fn get_at(&self, key: &K, ts: Timestamp) -> (Option<V>, Timestamp);
    fn scan(&self, start: &K, end: &K, ts: Timestamp) -> Vec<(K, Option<V>, Timestamp)>;

    // === OCC Prepare/Commit/Abort ===

    fn prepare(
        &mut self,
        id: TransactionId,
        txn: SharedTransaction<K, V, Timestamp>,
        commit: Timestamp,
        dry_run: bool,
    ) -> PrepareResult<Timestamp>;

    /// Record a commit in the transaction log and apply the write set.
    /// Panics (debug only) if the transaction was previously logged as aborted
    /// or committed at a different timestamp.
    fn commit_and_log(
        &mut self,
        id: TransactionId,
        txn: &Transaction<K, V, Timestamp>,
        commit: Timestamp,
    );

    fn remove_prepared(&mut self, id: TransactionId) -> bool;

    fn add_prepared(
        &mut self,
        id: TransactionId,
        txn: SharedTransaction<K, V, Timestamp>,
        commit: Timestamp,
        finalized: bool,
    );

    // === Prepared Queries ===

    /// Look up a prepared transaction by ID.
    fn prepared_get(
        &self,
        id: &TransactionId,
    ) -> Option<(&Timestamp, &SharedTransaction<K, V, Timestamp>, bool)>;

    /// Check the current status of a transaction for prepare/check-prepare decisions.
    ///
    /// Performs a cascading lookup: txn_log → prepared_at_timestamp → min_prepare_time,
    /// returning a typed status that callers map to their own result type.
    fn check_prepare_status(&self, id: &TransactionId, commit: &Timestamp) -> CheckPrepareStatus;

    /// Mark a prepared transaction as finalized (IR quorum confirmed).
    /// Returns true if the entry existed at the given commit timestamp.
    fn set_prepared_finalized(&mut self, id: &TransactionId, commit: &Timestamp) -> bool;

    fn prepared_count(&self) -> usize;

    /// Returns the oldest prepared transaction (minimum commit timestamp).
    fn oldest_prepared(
        &self,
    ) -> Option<(TransactionId, Timestamp, SharedTransaction<K, V, Timestamp>)>;

    /// Remove all prepared transactions that are NOT finalized.
    fn remove_unfinalized_prepared(&mut self);

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

    // === Fast Path Validation ===

    fn get_validated(&self, key: &K, ts: Timestamp) -> Option<(Option<V>, Timestamp)>;
    fn scan_validated(
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
