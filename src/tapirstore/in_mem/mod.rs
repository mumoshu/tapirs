#[cfg(test)]
mod tests;

use crate::mvcc::backend::MvccBackend;
use crate::mvcc::disk::error::StorageError;
use crate::occ::{PrepareConflict, PrepareResult, SharedTransaction, Store as OccStore, Transaction, TransactionId};
use crate::tapir::{Key, LeaderRecordDelta, ShardNumber, Timestamp, Value};
use crate::tapirstore::{CheckPrepareStatus, MinPrepareTimes, RecordDeltaDuringView, TapirStore, TransactionLog};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// In-memory TapirStore wrapping OccStore + transaction log + min-prepare-time + CDC deltas.
///
/// This is a mechanical extraction of the state that previously lived inline in
/// TapirReplica. Each trait method delegates to the corresponding OccStore method
/// or BTreeMap operation.
#[derive(Serialize, Deserialize)]
pub struct InMemTapirStore<K, V, M> {
    #[serde(bound(
        serialize = "K: Key, V: Value, M: Serialize",
        deserialize = "K: Key, V: Value, M: Deserialize<'de>"
    ))]
    occ: OccStore<K, V, Timestamp, M>,

    transaction_log: TransactionLog,

    min_prepare_times: MinPrepareTimes,

    #[serde(bound(
        serialize = "K: Key, V: Value",
        deserialize = "K: Key, V: Value"
    ))]
    record_delta_during_view: RecordDeltaDuringView<K, V>,
}

impl<K: Key, V: Value, M> InMemTapirStore<K, V, M> {
    pub fn new_with_backend(shard: ShardNumber, linearizable: bool, backend: M) -> Self {
        Self {
            occ: OccStore::new_with_backend(shard, linearizable, backend),
            transaction_log: TransactionLog::new(),
            min_prepare_times: MinPrepareTimes::new(),
            record_delta_during_view: RecordDeltaDuringView::new(),
        }
    }

    /// Access the underlying OccStore directly (needed by TapirReplica for
    /// accessing `prepared` field during protocol operations like `tick`).
    pub fn occ(&self) -> &OccStore<K, V, Timestamp, M> {
        &self.occ
    }

    pub fn occ_mut(&mut self) -> &mut OccStore<K, V, Timestamp, M> {
        &mut self.occ
    }

    pub fn commit(
        &mut self,
        id: TransactionId,
        txn: &Transaction<K, V, Timestamp>,
        commit: Timestamp,
    ) where
        M: MvccBackend<K, V, Timestamp>,
    {
        self.occ.commit(id, txn, commit);
    }

    pub fn prepared_at_timestamp(&self, id: &TransactionId, commit: &Timestamp) -> Option<bool> {
        self.occ.prepared_txns.prepared_at_timestamp(id, commit)
    }

    pub fn min_prepared_timestamp(&self) -> Option<u64> {
        self.occ.prepared_txns.min_prepared_timestamp()
    }

}

#[cfg(test)]
impl<K: Key, V: Value, M> InMemTapirStore<K, V, M> {
    pub fn min_prepare_time(&self) -> u64 {
        self.min_prepare_times.min_prepare_time()
    }

    pub fn set_min_prepare_time(&mut self, time: u64) {
        self.min_prepare_times.set_min_prepare_time(time);
    }

    pub fn finalized_min_prepare_time(&self) -> u64 {
        self.min_prepare_times.finalized_min_prepare_time()
    }

    pub fn set_finalized_min_prepare_time(&mut self, time: u64) {
        self.min_prepare_times.set_finalized_min_prepare_time(time);
    }
}

impl<K, V, M> TapirStore<K, V> for InMemTapirStore<K, V, M>
where
    K: Key,
    V: Value,
    M: MvccBackend<K, V, Timestamp> + Serialize + DeserializeOwned + 'static,
{
    fn shard(&self) -> ShardNumber {
        self.occ.shard()
    }

    // === Uncommitted Reads ===

    fn do_uncommitted_get(&self, key: &K) -> Result<(Option<V>, Timestamp), StorageError> {
        Ok(self.occ.get(key))
    }

    fn do_uncommitted_get_at(&self, key: &K, ts: Timestamp) -> Result<(Option<V>, Timestamp), StorageError> {
        Ok(self.occ.get_at(key, ts))
    }

    fn do_uncommitted_scan(&self, start: &K, end: &K, ts: Timestamp) -> Result<Vec<(K, Option<V>, Timestamp)>, StorageError> {
        Ok(self.occ.scan(start, end, ts))
    }

    // === OCC Prepare/Commit/Abort ===

    fn try_prepare_txn(
        &mut self,
        id: TransactionId,
        txn: SharedTransaction<K, V, Timestamp>,
        commit: Timestamp,
    ) -> PrepareResult<Timestamp> {
        self.occ.try_prepare_txn(id, txn, commit)
    }

    fn commit_txn(
        &mut self,
        id: TransactionId,
        txn: &Transaction<K, V, Timestamp>,
        commit: Timestamp,
    ) {
        let old = self.transaction_log.txn_log_insert(id, commit, true);
        if let Some((ts, committed)) = old {
            debug_assert!(committed, "{id:?} aborted");
            debug_assert_eq!(ts, commit, "{id:?} committed at (different) {ts:?}");
        }
        self.occ.commit(id, txn, commit);
    }

    fn remove_prepared_txn(&mut self, id: TransactionId) -> bool {
        self.occ.remove_prepared(id)
    }

    fn add_or_replace_or_finalize_prepared_txn(
        &mut self,
        id: TransactionId,
        txn: SharedTransaction<K, V, Timestamp>,
        commit: Timestamp,
        finalized: bool,
    ) {
        self.occ
            .add_or_replace_or_finalize_prepared_txn(id, txn, commit, finalized);
    }

    // === Prepared Queries ===

    fn get_prepared_txn(
        &self,
        id: &TransactionId,
    ) -> Option<(&Timestamp, &SharedTransaction<K, V, Timestamp>, bool)> {
        self.occ.prepared_txns.get(id)
    }

    fn check_prepare_status(&self, id: &TransactionId, commit: &Timestamp) -> CheckPrepareStatus {
        self.occ.prepared_txns.check_prepare_status(
            self.transaction_log.txn_log_get(id),
            self.min_prepare_times.min_prepare_time(),
            id,
            commit,
        )
    }

    fn finalize_prepared_txn(&mut self, id: &TransactionId, commit: &Timestamp) -> bool {
        self.occ.prepared_txns.finalize(id, commit)
    }

    fn prepared_count(&self) -> usize {
        self.occ.prepared_txns.len()
    }

    fn get_oldest_prepared_txn(
        &self,
    ) -> Option<(TransactionId, Timestamp, SharedTransaction<K, V, Timestamp>)> {
        self.occ.prepared_txns.oldest()
    }

    fn remove_all_unfinalized_prepared_txns(&mut self) {
        self.occ.prepared_txns.remove_all_unfinalized();
    }

    // === Committed Read/Scan ===

    fn do_committed_get(
        &mut self,
        key: K,
        ts: Timestamp,
    ) -> Result<(Option<V>, Timestamp), PrepareConflict> {
        self.occ.quorum_read(key, ts)
    }

    fn do_committed_scan(
        &mut self,
        start: K,
        end: K,
        ts: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, PrepareConflict> {
        self.occ.quorum_scan(start, end, ts)
    }

    // === Uncommitted Validated Reads ===

    fn do_uncommitted_get_validated(&self, key: &K, ts: Timestamp) -> Option<(Option<V>, Timestamp)> {
        self.occ.get_validated(key, ts)
    }

    fn do_uncommitted_scan_validated(
        &self,
        start: &K,
        end: &K,
        ts: Timestamp,
    ) -> Option<Vec<(K, Option<V>, Timestamp)>> {
        self.occ.scan_validated(start, end, ts)
    }

    // === Transaction Log ===

    fn txn_log_get(&self, id: &TransactionId) -> Option<(Timestamp, bool)> {
        self.transaction_log.txn_log_get(id)
    }

    fn txn_log_insert(
        &mut self,
        id: TransactionId,
        ts: Timestamp,
        committed: bool,
    ) -> Option<(Timestamp, bool)> {
        self.transaction_log.txn_log_insert(id, ts, committed)
    }

    fn txn_log_contains(&self, id: &TransactionId) -> bool {
        self.transaction_log.txn_log_contains(id)
    }

    fn txn_log_len(&self) -> usize {
        self.transaction_log.txn_log_len()
    }

    // === Min Prepare Time ===

    fn raise_min_prepare_time(&mut self, time: u64) -> u64 {
        let min_prepared_ts = self.min_prepared_timestamp();
        self.min_prepare_times.raise(time, min_prepared_ts)
    }

    fn finalize_min_prepare_time(&mut self, time: u64) {
        self.min_prepare_times.finalize(time);
    }

    fn sync_min_prepare_time(&mut self, time: u64) {
        self.min_prepare_times.sync(time);
    }

    fn reset_min_prepare_time_to_finalized(&mut self) {
        self.min_prepare_times.reset_to_finalized();
    }

    // === CDC Deltas ===

    fn record_cdc_delta(&mut self, base_view: u64, delta: LeaderRecordDelta<K, V>) {
        self.record_delta_during_view.record_cdc_delta(base_view, delta);
    }

    fn cdc_deltas_from(&self, from_view: u64) -> Vec<LeaderRecordDelta<K, V>> {
        self.record_delta_during_view.cdc_deltas_from(from_view)
    }

    fn cdc_max_view(&self) -> Option<u64> {
        self.record_delta_during_view.cdc_max_view()
    }

    // === Resharding ===

    fn min_prepare_baseline(&self) -> Option<Timestamp> {
        self.occ.min_prepare_baseline()
    }
}
