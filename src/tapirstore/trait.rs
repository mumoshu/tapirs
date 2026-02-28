use crate::occ::{PrepareConflict, PrepareResult, SharedTransaction, Transaction, TransactionId};
use crate::tapir::{Key, LeaderRecordDelta, ShardNumber, Timestamp, Value};
use serde::{de::DeserializeOwned, Serialize};

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

    fn commit(
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

    /// Check if a transaction is prepared at a specific commit timestamp.
    /// Returns `Some(finalized)` if prepared at that timestamp, `None` otherwise.
    fn prepared_at_timestamp(&self, id: &TransactionId, commit: &Timestamp) -> Option<bool>;

    /// Mark a prepared transaction as finalized (IR quorum confirmed).
    /// Returns true if the entry existed at the given commit timestamp.
    fn set_prepared_finalized(&mut self, id: &TransactionId, commit: &Timestamp) -> bool;

    fn prepared_count(&self) -> usize;

    /// Returns the minimum commit timestamp among all prepared transactions.
    fn min_prepared_timestamp(&self) -> Option<u64>;

    /// Returns the oldest prepared transaction (minimum commit timestamp).
    fn oldest_prepared(
        &self,
    ) -> Option<(TransactionId, Timestamp, SharedTransaction<K, V, Timestamp>)>;

    /// Remove all prepared transactions that are NOT finalized.
    fn remove_unfinalized_prepared(&mut self);

    // === Quorum Read/Scan ===

    fn quorum_read(
        &mut self,
        key: K,
        ts: Timestamp,
    ) -> Result<(Option<V>, Timestamp), PrepareConflict>;

    fn quorum_scan(
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

    fn min_prepare_time(&self) -> u64;
    fn set_min_prepare_time(&mut self, time: u64);

    /// Raise tentative min_prepare_time to max(current, min(time, min_prepared_timestamp)).
    /// Returns the final min_prepare_time value.
    fn raise_min_prepare_time(&mut self, time: u64) -> u64;
    fn finalized_min_prepare_time(&self) -> u64;
    fn set_finalized_min_prepare_time(&mut self, time: u64);

    // === CDC Deltas ===

    fn record_cdc_delta(&mut self, base_view: u64, delta: LeaderRecordDelta<K, V>);
    fn cdc_deltas_from(&self, from_view: u64) -> Vec<LeaderRecordDelta<K, V>>;
    fn cdc_max_view(&self) -> Option<u64>;

    // === Resharding ===

    fn min_prepare_baseline(&self) -> (Option<Timestamp>, Option<Timestamp>);
}
