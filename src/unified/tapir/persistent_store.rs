use crate::ir::OpId;
use crate::mvcc::disk::disk_io::DiskIo;
use crate::mvcc::disk::error::StorageError;
use crate::occ::{
    PrepareConflict, PrepareResult, SharedTransaction, Transaction as OccTransaction,
    TransactionId,
};
use crate::tapir::{Key, LeaderRecordDelta, ShardNumber, Timestamp, Value};
use crate::tapir::store::{CheckPrepareStatus, MinPrepareTimes, RecordDeltaDuringView, TapirStore};
use crate::unified::tapir::store::TapirState;
use std::collections::BTreeSet;
use std::sync::Arc;

/// VlogLsm-backed TapirStore implementation.
///
/// Wraps `TapirState` (pure persistence + OCC layer) with runtime-only
/// protocol state: `finalized_prepares` tracks which prepared transactions
/// have been confirmed by IR consensus, and `min_prepare_times` provides
/// tentative/finalized lower bounds on acceptable prepare timestamps.
///
/// Neither runtime field is persisted — both are rebuilt during view
/// change sync/merge. On recovery, all persisted prepared entries are
/// treated as finalized.
pub struct PersistentTapirStore<K: Ord, V, IO: DiskIo> {
    pub(crate) state: TapirState<K, V, IO>,
    finalized_prepares: BTreeSet<TransactionId>,
    min_prepare_times: MinPrepareTimes,
    /// Cached count of entries in committed VlogLsm (commits + aborts).
    txn_log_count: usize,
    /// CDC deltas recorded during each view. Runtime-only — rebuilt during
    /// view change sync/merge.
    record_delta_during_view: RecordDeltaDuringView<K, V>,
}

impl<
        K: Key + serde::Serialize + serde::de::DeserializeOwned,
        V: Value + serde::Serialize + serde::de::DeserializeOwned,
        IO: DiskIo,
    > PersistentTapirStore<K, V, IO>
{
    pub(crate) fn new(state: TapirState<K, V, IO>) -> Self {
        // On recovery, treat all persisted prepared entries as finalized.
        let min_key = TransactionId {
            client_id: crate::IrClientId(0),
            number: 0,
        };
        let mut finalized_prepares = BTreeSet::new();
        let index_keys: Vec<TransactionId> = state
            .prepared_memtable_keys(&min_key)
            .chain(state.prepared_index_keys())
            .collect();
        for txn_id in &index_keys {
            if state.get_prepared_txn(txn_id).is_some() {
                finalized_prepares.insert(*txn_id);
            }
        }

        // Recover txn_log_count: persisted count + unsealed memtable entries.
        let persisted_count = state.manifest_txn_log_count() as usize;
        let memtable_count = state
            .committed_memtable_keys(&min_key)
            .count();
        let txn_log_count = persisted_count + memtable_count;

        Self {
            state,
            finalized_prepares,
            min_prepare_times: MinPrepareTimes::new(),
            txn_log_count,
            record_delta_during_view: RecordDeltaDuringView::new(),
        }
    }

    /// Minimum commit timestamp among all prepared transactions.
    fn min_prepared_timestamp(&self) -> Option<u64> {
        let min_key = TransactionId {
            client_id: crate::IrClientId(0),
            number: 0,
        };
        let mut keys: BTreeSet<TransactionId> = self
            .state
            .prepared_memtable_keys(&min_key)
            .collect();
        keys.extend(self.state.prepared_index_keys());

        keys.iter()
            .filter_map(|k| self.state.get_prepared_txn(k))
            .map(|(ts, _)| ts.time)
            .min()
    }
}

impl<
        K: Key + serde::Serialize + serde::de::DeserializeOwned,
        V: Value + serde::Serialize + serde::de::DeserializeOwned,
        IO: DiskIo + Sync,
    > TapirStore<K, V> for PersistentTapirStore<K, V, IO>
{
    type Payload = crate::unified::ir::ir_record_store::PersistentPayload<
        crate::tapir::IO<K, V>,
        crate::tapir::CO<K, V>,
        crate::tapir::CR,
    >;

    fn shard(&self) -> ShardNumber {
        self.state.shard
    }

    // === Uncommitted Reads ===

    fn do_uncommitted_get(&self, key: &K) -> Result<(Option<V>, Timestamp), StorageError> {
        self.state.snapshot_get(key)
    }

    fn do_uncommitted_get_at(
        &self,
        key: &K,
        ts: Timestamp,
    ) -> Result<(Option<V>, Timestamp), StorageError> {
        self.state.snapshot_get_at(key, ts)
    }

    fn do_uncommitted_scan(
        &self,
        start: &K,
        end: &K,
        ts: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, StorageError> {
        self.state.snapshot_scan(start, end, ts)
    }

    // === OCC Prepare/Commit/Abort ===

    fn try_prepare_txn(
        &mut self,
        _op_id: OpId,
        id: TransactionId,
        txn: SharedTransaction<K, V, Timestamp>,
        commit: Timestamp,
    ) -> PrepareResult<Timestamp> {
        match self.state.prepare(id, &txn, commit) {
            Ok(result) => result,
            Err(_) => PrepareResult::Fail,
        }
    }

    fn commit_txn(
        &mut self,
        _op_id: OpId,
        id: TransactionId,
        txn: &OccTransaction<K, V, Timestamp>,
        commit: Timestamp,
    ) {
        let old = self.txn_log_insert(id, commit, true);
        if let Some((ts, committed)) = old {
            debug_assert!(committed, "{id:?} aborted");
            debug_assert_eq!(ts, commit, "{id:?} committed at (different) {ts:?}");
        }

        let shard = self.state.shard;
        let read_set: Vec<(K, Timestamp)> = txn
            .shard_read_set(shard)
            .map(|(k, ts)| (k.clone(), ts))
            .collect();
        let write_set: Vec<(K, Option<V>)> = txn
            .shard_write_set(shard)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let scan_set: Vec<(K, K, Timestamp)> = txn
            .shard_scan_set(shard)
            .map(|e| (e.start_key.clone(), e.end_key.clone(), e.timestamp))
            .collect();

        self.state
            .commit(id, &read_set, &write_set, &scan_set, commit)
            .expect("commit should not fail");

        self.finalized_prepares.remove(&id);
    }

    fn remove_prepared_txn(&mut self, id: TransactionId) -> bool {
        let existed = self.state.get_prepared_txn(&id).is_some();
        if existed {
            self.state.abort(&id);
        }
        self.finalized_prepares.remove(&id);
        existed
    }

    fn add_or_replace_or_finalize_prepared_txn(
        &mut self,
        _op_id: OpId,
        id: TransactionId,
        txn: SharedTransaction<K, V, Timestamp>,
        commit: Timestamp,
        finalized: bool,
    ) {
        if let Some((existing_ts, _)) = self.state.get_prepared_txn(&id) {
            if existing_ts == commit {
                // Same timestamp — just update finalized flag.
                if finalized {
                    self.finalized_prepares.insert(id);
                }
                return;
            }
            // Different timestamp — remove old, insert new.
            self.state.abort(&id);
        }
        let _ = self.state.prepare(id, &txn, commit);
        if finalized {
            self.finalized_prepares.insert(id);
        }
    }

    // === Prepared Queries ===

    fn get_prepared_txn(
        &self,
        id: &TransactionId,
    ) -> Option<(Timestamp, SharedTransaction<K, V, Timestamp>, bool)> {
        let (ts, txn) = self.state.get_prepared_txn(id)?;
        let finalized = self.finalized_prepares.contains(id);
        Some((ts, txn, finalized))
    }

    fn check_prepare_status(
        &self,
        id: &TransactionId,
        commit: &Timestamp,
    ) -> CheckPrepareStatus {
        // Cascading lookup: txn_log → prepared → min_prepare_time.
        if let Some((ts, committed)) = self.txn_log_get(id) {
            if committed {
                if ts == *commit {
                    return CheckPrepareStatus::CommittedAtTimestamp;
                } else {
                    return CheckPrepareStatus::CommittedDifferent {
                        proposed: ts.time,
                    };
                }
            } else {
                return CheckPrepareStatus::Aborted;
            }
        }

        if let Some((existing_ts, _)) = self.state.get_prepared_txn(id)
            && existing_ts == *commit
        {
            let finalized = self.finalized_prepares.contains(id);
            return CheckPrepareStatus::PreparedAtTimestamp { finalized };
        }

        let mpt = self.min_prepare_times.min_prepare_time();
        if commit.time < mpt
            || self
                .state
                .get_prepared_txn(id)
                .map(|(ts, _)| ts.time < mpt)
                .unwrap_or(false)
        {
            return CheckPrepareStatus::TooLate;
        }

        CheckPrepareStatus::Unknown
    }

    fn finalize_prepared_txn(&mut self, id: &TransactionId, commit: &Timestamp) -> bool {
        if let Some((ts, _)) = self.state.get_prepared_txn(id)
            && ts == *commit
        {
            self.finalized_prepares.insert(*id);
            return true;
        }
        false
    }

    fn prepared_count(&self) -> usize {
        self.state.prepared_count()
    }

    fn get_oldest_prepared_txn(
        &self,
    ) -> Option<(TransactionId, Timestamp, SharedTransaction<K, V, Timestamp>)> {
        let min_key = TransactionId {
            client_id: crate::IrClientId(0),
            number: 0,
        };
        let mut keys: BTreeSet<TransactionId> = self
            .state
            .prepared_memtable_keys(&min_key)
            .collect();
        keys.extend(self.state.prepared_index_keys());

        keys.iter()
            .filter_map(|k| {
                let (ts, txn) = self.state.get_prepared_txn(k)?;
                Some((*k, ts, txn))
            })
            .min_by_key(|(_, ts, _)| *ts)
    }

    fn remove_all_unfinalized_prepared_txns(&mut self) {
        let min_key = TransactionId {
            client_id: crate::IrClientId(0),
            number: 0,
        };
        let mut keys: BTreeSet<TransactionId> = self
            .state
            .prepared_memtable_keys(&min_key)
            .collect();
        keys.extend(self.state.prepared_index_keys());

        let to_remove: Vec<TransactionId> = keys
            .iter()
            .filter(|k| !self.finalized_prepares.contains(k))
            .filter(|k| self.state.get_prepared_txn(k).is_some())
            .copied()
            .collect();

        for id in to_remove {
            self.state.abort(&id);
        }
    }

    // === Committed Read/Scan ===

    fn do_committed_get(
        &mut self,
        key: K,
        ts: Timestamp,
    ) -> Result<(Option<V>, Timestamp), PrepareConflict> {
        self.state.snapshot_get_protected(&key, ts)
    }

    fn do_committed_scan(
        &mut self,
        start: K,
        end: K,
        ts: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, PrepareConflict> {
        self.state.snapshot_scan_protected(&start, &end, ts)
    }

    // === Uncommitted Validated Reads ===

    fn do_uncommitted_get_validated(
        &self,
        key: &K,
        ts: Timestamp,
    ) -> Option<(Option<V>, Timestamp)> {
        // Check if last_read_ts >= ts for the version at snapshot_ts.
        let read_ts = self.state.get_last_read_ts_at(key, ts)?;
        if read_ts >= ts {
            let (value, write_ts) = self.state.snapshot_get_at(key, ts).ok()?;
            Some((value, write_ts))
        } else {
            None
        }
    }

    fn do_uncommitted_scan_validated(
        &self,
        start: &K,
        end: &K,
        ts: Timestamp,
    ) -> Option<Vec<(K, Option<V>, Timestamp)>> {
        // Check if any recorded range_read covers [start, end] at ts.
        if !self.state.has_covering_range_read(start, end, ts) {
            return None;
        }
        self.state.snapshot_scan(start, end, ts).ok()
    }

    // === Transaction Log ===

    fn txn_log_get(&self, id: &TransactionId) -> Option<(Timestamp, bool)> {
        use crate::unified::tapir::types::TxnLogEntry;
        match self.state.committed_get(id) {
            Ok(Some(TxnLogEntry::Committed(txn))) => Some((txn.commit_ts, true)),
            Ok(Some(TxnLogEntry::Aborted(ts))) => Some((ts, false)),
            _ => None,
        }
    }

    fn txn_log_insert(
        &mut self,
        id: TransactionId,
        ts: Timestamp,
        committed: bool,
    ) -> Option<(Timestamp, bool)> {
        use crate::unified::tapir::types::TxnLogEntry;
        let old = self.txn_log_get(&id);
        if committed {
            // For standalone txn_log_insert(committed=true), store an empty
            // Transaction stub. In practice, commit_txn() calls commit() which
            // stores the real transaction — this path is only hit by
            // conformance tests.
            let arc_txn = Arc::new(crate::unified::tapir::Transaction {
                transaction_id: id,
                commit_ts: ts,
                read_set: Vec::new(),
                write_set: Vec::new(),
                scan_set: Vec::new(),
            });
            self.state.committed_put(id, TxnLogEntry::Committed(arc_txn));
        } else {
            self.state.committed_put(id, TxnLogEntry::Aborted(ts));
        }
        if old.is_none() {
            self.txn_log_count += 1;
            self.state.set_manifest_txn_log_count(self.txn_log_count as u64);
        }
        old
    }

    fn txn_log_contains(&self, id: &TransactionId) -> bool {
        matches!(self.state.committed_get(id), Ok(Some(_)))
    }

    fn txn_log_len(&self) -> usize {
        self.txn_log_count
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
        self.record_delta_during_view
            .record_cdc_delta(base_view, delta);
    }

    fn cdc_deltas_from(&self, from_view: u64) -> Vec<LeaderRecordDelta<K, V>> {
        self.record_delta_during_view.cdc_deltas_from(from_view)
    }

    fn cdc_max_view(&self) -> Option<u64> {
        self.record_delta_during_view.cdc_max_view()
    }

    // === Resharding ===

    fn min_prepare_baseline(&self) -> Option<Timestamp> {
        self.state.max_read_time()
    }

    fn flush(&mut self) {
        // Delegates to TapirState::seal_current_view which seals all 3 VlogLsms
        // (committed, prepared, mvcc) and saves the manifest.
        self.state
            .seal_current_view(0)
            .expect("PersistentTapirStore::flush: seal failed");
    }

    fn stored_bytes(&self) -> Option<u64> {
        Some(self.state.stored_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::disk::disk_io::OpenFlags;
    use crate::mvcc::disk::memory_io::MemoryIo;
    use crate::unified::tapir::store;

    fn new_store() -> (std::path::PathBuf, PersistentTapirStore<String, String, MemoryIo>) {
        let base_dir = MemoryIo::temp_path();
        let state = store::open::<String, String, MemoryIo>(
            &base_dir,
            OpenFlags { create: true, direct: false },
            ShardNumber(0),
            true,
        )
        .unwrap();
        let wrapper = PersistentTapirStore::new(state);
        (base_dir, wrapper)
    }

    crate::tapir_store_conformance_tests!(new_store());

    mod ir_conformance {
        use crate::discovery::InMemoryShardDirectory;
        use crate::mvcc::disk::disk_io::OpenFlags;
        use crate::mvcc::disk::memory_io::MemoryIo;
        use crate::tapir;
        use crate::unified::ir::ir_record_store::PersistentIrRecordStore;
        use crate::unified::tapir::persistent_store::PersistentTapirStore;
        use crate::unified::tapir::store;
        use crate::{ChannelRegistry, ShardNumber};
        use std::sync::Arc;

        type S = PersistentTapirStore<String, String, MemoryIo>;
        type U = tapir::Replica<String, String, S>;
        type R = PersistentIrRecordStore<
            tapir::IO<String, String>,
            tapir::CO<String, String>,
            tapir::CR,
            MemoryIo,
        >;

        fn persistent_factory() -> (
            ChannelRegistry<U>,
            Arc<InMemoryShardDirectory<usize>>,
            impl FnMut() -> (U, R),
        ) {
            let registry = ChannelRegistry::default();
            let directory = Arc::new(InMemoryShardDirectory::new());
            let factory = || {
                let base_dir = MemoryIo::temp_path();
                let flags = OpenFlags {
                    create: true,
                    direct: false,
                };
                let state = store::open::<String, String, MemoryIo>(
                    &base_dir,
                    flags,
                    ShardNumber(0),
                    true,
                )
                .unwrap();
                let upcalls = tapir::Replica::new_with_store(PersistentTapirStore::new(state));
                let record = PersistentIrRecordStore::open(&base_dir, flags).unwrap();
                (upcalls, record)
            };
            (registry, directory, factory)
        }

        crate::ir_replica_conformance_tests!(persistent_factory);
    }
}
