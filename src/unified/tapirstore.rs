use crate::mvcc::disk::disk_io::DiskIo;
use crate::mvcc::disk::error::StorageError;
use crate::occ::{PrepareConflict, PrepareResult, SharedTransaction, Transaction, TransactionId};
use crate::tapir::{Key, LeaderRecordDelta, ShardNumber, Timestamp, Value};
use crate::tapirstore::{CheckPrepareStatus, TapirStore};

use super::UnifiedStore;

impl<K: Key, V: Value, IO: DiskIo> TapirStore<K, V> for UnifiedStore<K, V, IO> {
    // === Identity ===

    fn shard(&self) -> ShardNumber {
        todo!()
    }

    // === Uncommitted Reads ===

    fn do_uncommitted_get(&self, key: &K) -> Result<(Option<V>, Timestamp), StorageError> {
        if let Some((ck, entry)) = self.unified_memtable().get_latest(key) {
            let ts = ck.timestamp.0;
            let value = self.resolve_value(entry)?;
            return Ok((value, ts));
        }
        Ok((None, Timestamp::default()))
    }

    fn do_uncommitted_get_at(&self, key: &K, ts: Timestamp) -> Result<(Option<V>, Timestamp), StorageError> {
        if let Some((ck, entry)) = self.unified_memtable().get_at(key, ts) {
            let write_ts = ck.timestamp.0;
            let value = self.resolve_value(entry)?;
            return Ok((value, write_ts));
        }
        Ok((None, Timestamp::default()))
    }

    fn do_uncommitted_scan(
        &self,
        start: &K,
        end: &K,
        ts: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, StorageError> {
        let results = self.unified_memtable().scan(start, end, ts);
        let mut output = Vec::new();
        for (ck, entry) in results {
            let write_ts = ck.timestamp.0;
            let value = self.resolve_value(entry)?;
            output.push((ck.key.clone(), value, write_ts));
        }
        Ok(output)
    }

    // === OCC Prepare/Commit/Abort ===

    fn try_prepare_txn(
        &mut self,
        _id: TransactionId,
        _txn: SharedTransaction<K, V, Timestamp>,
        _commit: Timestamp,
    ) -> PrepareResult<Timestamp> {
        todo!()
    }

    fn commit_txn(
        &mut self,
        _id: TransactionId,
        _txn: &Transaction<K, V, Timestamp>,
        _commit: Timestamp,
    ) {
        todo!()
    }

    fn remove_prepared_txn(&mut self, _id: TransactionId) -> bool {
        todo!()
    }

    fn add_or_replace_or_finalize_prepared_txn(
        &mut self,
        _id: TransactionId,
        _txn: SharedTransaction<K, V, Timestamp>,
        _commit: Timestamp,
        _finalized: bool,
    ) {
        todo!()
    }

    // === Prepared Queries ===

    fn get_prepared_txn(
        &self,
        _id: &TransactionId,
    ) -> Option<(&Timestamp, &SharedTransaction<K, V, Timestamp>, bool)> {
        todo!()
    }

    fn check_prepare_status(
        &self,
        _id: &TransactionId,
        _commit: &Timestamp,
    ) -> CheckPrepareStatus {
        todo!()
    }

    fn finalize_prepared_txn(&mut self, _id: &TransactionId, _commit: &Timestamp) -> bool {
        todo!()
    }

    fn prepared_count(&self) -> usize {
        todo!()
    }

    fn get_oldest_prepared_txn(
        &self,
    ) -> Option<(TransactionId, Timestamp, SharedTransaction<K, V, Timestamp>)> {
        todo!()
    }

    fn remove_all_unfinalized_prepared_txns(&mut self) {
        todo!()
    }

    // === Committed Read/Scan ===

    fn do_committed_get(
        &mut self,
        _key: K,
        _ts: Timestamp,
    ) -> Result<(Option<V>, Timestamp), PrepareConflict> {
        todo!()
    }

    fn do_committed_scan(
        &mut self,
        _start: K,
        _end: K,
        _ts: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, PrepareConflict> {
        todo!()
    }

    // === Uncommitted Validated Reads ===

    fn do_uncommitted_get_validated(
        &self,
        _key: &K,
        _ts: Timestamp,
    ) -> Option<(Option<V>, Timestamp)> {
        todo!()
    }

    fn do_uncommitted_scan_validated(
        &self,
        _start: &K,
        _end: &K,
        _ts: Timestamp,
    ) -> Option<Vec<(K, Option<V>, Timestamp)>> {
        todo!()
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
        let min_prepared_ts = self
            .prepare_registry
            .values()
            .map(|p| p.commit_ts.time)
            .min();
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
        todo!()
    }
}
