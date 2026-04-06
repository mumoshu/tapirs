use crate::ir::OpId;
use crate::mvcc::disk::disk_io::DiskIo;
use crate::mvcc::disk::error::StorageError;
use crate::mvcc::disk::memtable::{CompositeKey, MaxValue};
use crate::occ::{
    PrepareConflict, PrepareResult, SharedTransaction,
    Transaction as OccTransaction, TransactionId,
};
use crate::tapir::{Key, LeaderRecordDelta, ShardNumber, Timestamp, Value};
use crate::tapir::store::{CheckPrepareStatus, TapirStore};
use crate::unified::ir::ir_record_store::PersistentPayload;
use crate::unified::tapir::occ_cache::MvccQueries;
use crate::unified::tapir::storage_types::MvccIndexEntry;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use super::{CombinedStoreInner, PreparedRef, TxnLogRef};

/// Handle for the TAPIR store side of a [`CombinedStoreInner`].
///
/// Wraps `Arc<Mutex<CombinedStoreInner>>` (shared with
/// [`CombinedRecordHandle`](super::record_handle::CombinedRecordHandle))
/// and implements [`TapirStore`]. Transaction data is never duplicated into
/// TAPIR VlogLsms — prepared transactions resolve lazily from con_lsm
/// (PreparedRef.op_id → CO::Prepare → transaction), committed transactions
/// resolve lazily from inc_lsm (TxnLogRef.op_id → IO::Commit → transaction).
pub struct CombinedTapirHandle<K: Ord, V, DIO: DiskIo> {
    pub(crate) inner: Arc<Mutex<CombinedStoreInner<K, V, DIO>>>,
}

impl<K: Ord + Debug, V: Debug, DIO: DiskIo> Debug for CombinedTapirHandle<K, V, DIO> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CombinedTapirHandle").finish()
    }
}

// ---------------------------------------------------------------------------
// MvccView: borrow-splitting wrapper identical to TapirState's version.
// Allows occ_cache to be borrowed independently from the MVCC VlogLsm.
// ---------------------------------------------------------------------------

struct MvccView<'a, K: Ord, IO: DiskIo> {
    mvcc: &'a crate::unified::wisckeylsm::lsm::VlogLsm<
        CompositeKey<K, Timestamp>,
        MvccIndexEntry,
        IO,
    >,
}

impl<K: Key + Serialize + DeserializeOwned, IO: DiskIo> MvccQueries<K> for MvccView<'_, K, IO> {
    fn get_version_range(
        &self,
        key: &K,
        read_ts: Timestamp,
    ) -> Result<(Timestamp, Option<Timestamp>), StorageError> {
        let search = CompositeKey::new(key.clone(), read_ts);
        let at_ts = if let Some((ck, _)) = self.mvcc.range_get_first(&search)?
            && ck.key == *key
        {
            ck.timestamp.0
        } else {
            Timestamp::default()
        };

        let start = CompositeKey::new(key.clone(), Timestamp::max_value());
        let mut next_ts = None;
        for (ck, _) in self.mvcc.memtable_range_from(&start) {
            if ck.key != *key {
                break;
            }
            if ck.timestamp.0 > at_ts {
                next_ts = Some(match next_ts {
                    Some(prev) if ck.timestamp.0 < prev => ck.timestamp.0,
                    Some(prev) => prev,
                    None => ck.timestamp.0,
                });
            }
        }
        for (ck, _) in self.mvcc.index_range(start..) {
            if ck.key != *key {
                break;
            }
            if ck.timestamp.0 > at_ts {
                next_ts = Some(match next_ts {
                    Some(prev) if ck.timestamp.0 < prev => ck.timestamp.0,
                    Some(prev) => prev,
                    None => ck.timestamp.0,
                });
            }
        }
        Ok((at_ts, next_ts))
    }

    fn get_last_read_ts(&self, key: &K) -> Result<Option<Timestamp>, StorageError> {
        let search = CompositeKey::new(key.clone(), Timestamp::max_value());
        if let Some((ck, entry)) = self.mvcc.range_get_first(&search)?
            && ck.key == *key
        {
            Ok(entry.last_read_ts.map(|t| Timestamp {
                time: t,
                client_id: crate::IrClientId(u64::MAX),
            }))
        } else {
            Ok(None)
        }
    }

    fn get_last_read_ts_at(
        &self,
        key: &K,
        at_ts: Timestamp,
    ) -> Result<Option<Timestamp>, StorageError> {
        let search = CompositeKey::new(key.clone(), at_ts);
        if let Some((ck, entry)) = self.mvcc.range_get_first(&search)?
            && ck.key == *key
        {
            Ok(entry.last_read_ts.map(|t| Timestamp {
                time: t,
                client_id: crate::IrClientId(u64::MAX),
            }))
        } else {
            Ok(None)
        }
    }

    fn has_writes_in_range(
        &self,
        start: &K,
        end: &K,
        after_ts: Timestamp,
        before_ts: Timestamp,
    ) -> Result<bool, StorageError> {
        let from_ck = CompositeKey::new(start.clone(), Timestamp::max_value());
        for (ck, _) in self.mvcc.memtable_range_from(&from_ck) {
            if ck.key > *end {
                break;
            }
            if ck.timestamp.0 > after_ts && ck.timestamp.0 < before_ts {
                return Ok(true);
            }
        }
        for (ck, _) in self.mvcc.index_range(from_ck..) {
            if ck.key > *end {
                break;
            }
            if ck.timestamp.0 > after_ts && ck.timestamp.0 < before_ts {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_latest_version_ts(&self, key: &K) -> Result<Timestamp, StorageError> {
        let search = CompositeKey::new(key.clone(), Timestamp::max_value());
        if let Some((ck, _)) = self.mvcc.range_get_first(&search)?
            && ck.key == *key
        {
            Ok(ck.timestamp.0)
        } else {
            Ok(Timestamp::default())
        }
    }
}

// ---------------------------------------------------------------------------
// Helper methods on CombinedStoreInner (used by TapirStore impl)
// ---------------------------------------------------------------------------

impl<K: Key + Serialize + DeserializeOwned, V: Value + Serialize + DeserializeOwned, DIO: DiskIo>
    CombinedStoreInner<K, V, DIO>
{
    /// Clamp a snapshot timestamp to enforce the ghost filter.
    ///
    /// After restoring from a cross-shard consistent snapshot, some MVCC entries
    /// may exist on this shard but not on others (cross-shard transactions that
    /// committed here but not everywhere). The ghost filter defines a timestamp
    /// range (cutoff_ts, ceiling_ts] of such entries.
    ///
    /// This method clamps the search timestamp so that MVCC lookups skip the
    /// ghost range entirely:
    /// - ts <= cutoff_ts: no change (all shards have these entries)
    /// - ts in (cutoff_ts, ceiling_ts]: clamped to cutoff_ts (skip ghost range)
    /// - ts > ceiling_ts: no change (new writes after restore, all shards see them)
    ///
    /// Without this, reads at timestamps in the ghost range would return entries
    /// that other shards don't have, breaking cross-shard consistency.
    fn effective_snapshot_ts(&self, ts: Timestamp) -> Timestamp {
        match &self.ghost_filter {
            Some(gf) => Timestamp {
                time: gf.clamp_ts(ts.time),
                ..ts
            },
            None => ts,
        }
    }

    /// Resolve the value for an MVCC entry by chaining through the committed
    /// VlogLsm (TxnLogRef) and the IR inc_lsm (IO::Commit).
    fn resolve_value(&self, entry: &MvccIndexEntry) -> Result<Option<V>, StorageError> {
        // committed stores TxnLogRef; Committed(op_id) → inc_lsm → IO::Commit → transaction
        if let Some(TxnLogRef::Committed { op_id, .. }) = self.committed.get(&entry.txn_id)?
            && let Some(inc_entry) = self.ir.inc_lsm().get(&op_id)?
            && let crate::tapir::IO::Commit { transaction, .. } = &inc_entry.op
        {
            let write_set: Vec<(K, Option<V>)> = transaction
                .shard_write_set(self.shard)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            return Ok(write_set
                .get(entry.write_index as usize)
                .and_then(|(_, v)| v.clone()));
        }
        Ok(None)
    }

    /// Resolve a prepared transaction lazily from con_lsm via PreparedRef.op_id.
    fn resolve_prepared_txn(
        &self,
        txn_id: &TransactionId,
    ) -> Option<(Timestamp, SharedTransaction<K, V, Timestamp>)> {
        match self.prepared.get(txn_id) {
            Ok(Some(Some(prep_ref))) => {
                // Resolve from con_lsm: op_id → ConsensusEntry → CO::Prepare → transaction
                if let Ok(Some(con_entry)) = self.ir.con_lsm().get(&prep_ref.op_id)
                    && let crate::tapir::CO::Prepare { transaction, .. } = con_entry.op
                {
                    return Some((prep_ref.commit_ts, transaction));
                }
                None
            }
            _ => None,
        }
    }

    // --- MVCC read helpers (same as TapirState) ---

    fn snapshot_get(&self, key: &K) -> Result<(Option<V>, Timestamp), StorageError> {
        let search = CompositeKey::new(key.clone(), Timestamp::max_value());
        if let Some((ck, entry)) = self.mvcc.range_get_first(&search)?
            && ck.key == *key
        {
            return Ok((self.resolve_value(&entry)?, ck.timestamp.0));
        }
        Ok((None, Timestamp::default()))
    }

    fn snapshot_get_at(
        &self,
        key: &K,
        ts: Timestamp,
    ) -> Result<(Option<V>, Timestamp), StorageError> {
        // Ghost filter: clamp ts to skip entries in the cross-shard inconsistent
        // range. Without this, a restored shard could return values that other
        // shards in the cluster don't have.
        let ts = self.effective_snapshot_ts(ts);
        let search = CompositeKey::new(key.clone(), ts);
        if let Some((ck, entry)) = self.mvcc.range_get_first(&search)?
            && ck.key == *key
        {
            return Ok((self.resolve_value(&entry)?, ck.timestamp.0));
        }
        Ok((None, Timestamp::default()))
    }

    fn snapshot_scan(
        &self,
        start: &K,
        end: &K,
        ts: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, StorageError> {
        // Ghost filter: clamp ts to skip entries in the cross-shard inconsistent
        // range. Without this, scan results would include ghost-range entries.
        let ts = self.effective_snapshot_ts(ts);
        let from_ck = CompositeKey::new(start.clone(), Timestamp::max_value());
        let mut merged: std::collections::BTreeMap<CompositeKey<K, Timestamp>, MvccIndexEntry> =
            std::collections::BTreeMap::new();
        for (ck, ptr) in self.mvcc.index_range(from_ck.clone()..) {
            if ck.key > *end {
                break;
            }
            let entry: MvccIndexEntry = self.mvcc.read_value_from_vlog(ptr)?;
            merged.insert(ck.clone(), entry);
        }
        for (ck, entry) in self.mvcc.memtable_range_from(&from_ck) {
            if ck.key > *end {
                break;
            }
            merged.insert(ck.clone(), entry.clone());
        }

        let mut out = Vec::new();
        let mut last_key: Option<&K> = None;
        for (ck, entry) in &merged {
            if ck.timestamp.0 > ts {
                continue;
            }
            if last_key == Some(&ck.key) {
                continue;
            }
            last_key = Some(&ck.key);
            let value = self.resolve_value(entry)?;
            out.push((ck.key.clone(), value, ck.timestamp.0));
        }
        Ok(out)
    }

    fn snapshot_get_protected(
        &mut self,
        key: &K,
        snapshot_ts: Timestamp,
    ) -> Result<(Option<V>, Timestamp), PrepareConflict> {
        if self.occ_cache.check_get_conflict(key, snapshot_ts) {
            return Err(PrepareConflict);
        }

        let (value, ts) = self
            .snapshot_get_at(key, snapshot_ts)
            .map_err(|_| PrepareConflict)?;

        if self.linearizable {
            if value.is_some() {
                self.update_mvcc_last_read_ts(key, snapshot_ts, snapshot_ts.time);
            } else {
                self.occ_cache
                    .record_range_read(key.clone(), key.clone(), snapshot_ts);
            }
            self.occ_cache.update_max_read_time(snapshot_ts);
        }

        Ok((value, ts))
    }

    fn snapshot_scan_protected(
        &mut self,
        start: &K,
        end: &K,
        snapshot_ts: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, PrepareConflict> {
        if self.occ_cache.check_scan_conflict(start, end, snapshot_ts) {
            return Err(PrepareConflict);
        }

        let results = self
            .snapshot_scan(start, end, snapshot_ts)
            .map_err(|_| PrepareConflict)?;

        if self.linearizable {
            self.occ_cache
                .record_range_read(start.clone(), end.clone(), snapshot_ts);
            self.occ_cache.update_max_read_time(snapshot_ts);
        }

        Ok(results)
    }

    // --- MVCC mutation helpers ---

    fn insert_mvcc_entry(
        &mut self,
        key: K,
        ts: Timestamp,
        txn_id: TransactionId,
        write_index: u16,
    ) {
        let ck = CompositeKey::new(key, ts);
        self.mvcc.put(
            ck,
            MvccIndexEntry {
                txn_id,
                write_index,
                last_read_ts: None,
            },
        );
    }

    fn update_mvcc_last_read_ts(&mut self, key: &K, read: Timestamp, commit_time: u64) {
        let search = CompositeKey::new(key.clone(), read);
        if let Ok(Some((found_ck, mut entry))) = self.mvcc.range_get_first(&search)
            && found_ck.key == *key
        {
            let old = entry.last_read_ts;
            entry.last_read_ts = Some(match entry.last_read_ts {
                Some(existing) => existing.max(commit_time),
                None => commit_time,
            });
            eprintln!("[MVCC-UPDATE-LRT] key={key:?} read={read:?} found_ts={:?} old_lrt={old:?} new_lrt={:?} commit_time={commit_time}", found_ck.timestamp.0, entry.last_read_ts);
            self.mvcc.put(found_ck, entry);
        }
    }

    // --- Prepare/commit/abort ---

    fn do_prepare(
        &mut self,
        op_id: OpId,
        txn_id: TransactionId,
        txn: &OccTransaction<K, V, Timestamp>,
        commit_ts: Timestamp,
    ) -> Result<PrepareResult<Timestamp>, StorageError> {
        let read_set: Vec<(K, Timestamp)> = txn
            .shard_read_set(self.shard)
            .map(|(k, ts)| (k.clone(), ts))
            .collect();
        let write_set: Vec<(K, Option<V>)> = txn
            .shard_write_set(self.shard)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let scan_set: Vec<(K, K, Timestamp)> = txn
            .shard_scan_set(self.shard)
            .map(|e| (e.start_key.clone(), e.end_key.clone(), e.timestamp))
            .collect();

        let mvcc_view = MvccView { mvcc: &self.mvcc };
        let result =
            self.occ_cache
                .occ_check(&mvcc_view, &read_set, &write_set, &scan_set, commit_ts)?;

        if !result.is_ok() {
            return Ok(result);
        }

        self.occ_cache
            .register_prepare(&read_set, &write_set, commit_ts);
        // Store lightweight PreparedRef instead of the full transaction.
        let prep_ref = PreparedRef {
            commit_ts,
            op_id,
            finalized: false,
        };
        self.prepared.put(txn_id, Some(prep_ref));
        Ok(PrepareResult::Ok)
    }

    fn do_commit(
        &mut self,
        op_id: OpId,
        txn_id: TransactionId,
        txn: &OccTransaction<K, V, Timestamp>,
        commit: Timestamp,
    ) {
        let read_set: Vec<(K, Timestamp)> = txn
            .shard_read_set(self.shard)
            .map(|(k, ts)| (k.clone(), ts))
            .collect();
        let write_set: Vec<(K, Option<V>)> = txn
            .shard_write_set(self.shard)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        self.occ_cache
            .unregister_prepare(&read_set, &write_set, commit);
        self.prepared.put(txn_id, None);

        // Store lightweight TxnLogRef instead of the full transaction.
        self.committed.put(txn_id, TxnLogRef::Committed { op_id, commit });

        for (i, (key, _value)) in write_set.iter().enumerate() {
            self.insert_mvcc_entry(key.clone(), commit, txn_id, i as u16);
        }
        for (key, read) in &read_set {
            self.update_mvcc_last_read_ts(key, *read, commit.time);
        }
    }

    fn do_abort(&mut self, txn_id: &TransactionId) {
        if let Some((commit_ts, txn)) = self.resolve_prepared_txn(txn_id) {
            let read_set: Vec<(K, Timestamp)> = txn
                .shard_read_set(self.shard)
                .map(|(k, ts)| (k.clone(), ts))
                .collect();
            let write_set: Vec<(K, Option<V>)> = txn
                .shard_write_set(self.shard)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            self.occ_cache
                .unregister_prepare(&read_set, &write_set, commit_ts);
        }
        self.prepared.put(*txn_id, None);
    }

    // --- Prepared iteration helpers ---

    fn all_prepared_keys(&self) -> BTreeSet<TransactionId> {
        let min_key = TransactionId {
            client_id: crate::IrClientId(0),
            number: 0,
        };
        let mut keys: BTreeSet<TransactionId> = self
            .prepared
            .memtable_range_from(&min_key)
            .map(|(k, _)| *k)
            .collect();
        keys.extend(self.prepared.index_range(..).map(|(k, _)| *k));
        keys
    }

    fn prepared_count_inner(&self) -> usize {
        self.all_prepared_keys()
            .iter()
            .filter(|k| matches!(self.prepared.get(k), Ok(Some(Some(_)))))
            .count()
    }

    fn min_prepared_timestamp(&self) -> Option<u64> {
        self.all_prepared_keys()
            .iter()
            .filter_map(|k| self.resolve_prepared_txn(k))
            .map(|(ts, _)| ts.time)
            .min()
    }
}

// ---------------------------------------------------------------------------
// TapirStore implementation
// ---------------------------------------------------------------------------

impl<K, V, DIO> TapirStore<K, V> for CombinedTapirHandle<K, V, DIO>
where
    K: Key + Serialize + DeserializeOwned,
    V: Value + Serialize + DeserializeOwned,
    DIO: DiskIo + Sync,
{
    type Payload = PersistentPayload<
        crate::tapir::IO<K, V>,
        crate::tapir::CO<K, V>,
        crate::tapir::CR,
    >;

    fn shard(&self) -> ShardNumber {
        self.inner.lock().unwrap().shard
    }

    // === Uncommitted Reads ===

    fn do_uncommitted_get(&self, key: &K) -> Result<(Option<V>, Timestamp), StorageError> {
        self.inner.lock().unwrap().snapshot_get(key)
    }

    fn do_uncommitted_get_at(
        &self,
        key: &K,
        ts: Timestamp,
    ) -> Result<(Option<V>, Timestamp), StorageError> {
        self.inner.lock().unwrap().snapshot_get_at(key, ts)
    }

    fn do_uncommitted_scan(
        &self,
        start: &K,
        end: &K,
        ts: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, StorageError> {
        self.inner.lock().unwrap().snapshot_scan(start, end, ts)
    }

    // === OCC Prepare/Commit/Abort ===

    fn try_prepare_txn(
        &mut self,
        op_id: OpId,
        id: TransactionId,
        txn: SharedTransaction<K, V, Timestamp>,
        commit: Timestamp,
    ) -> PrepareResult<Timestamp> {
        match self.inner.lock().unwrap().do_prepare(op_id, id, &txn, commit) {
            Ok(result) => result,
            Err(_) => PrepareResult::Fail,
        }
    }

    fn commit_txn(
        &mut self,
        op_id: OpId,
        id: TransactionId,
        txn: &OccTransaction<K, V, Timestamp>,
        commit: Timestamp,
    ) {
        let mut inner = self.inner.lock().unwrap();

        // Insert into transaction log (check for duplicates).
        let old = txn_log_get_inner(&inner, &id);
        if let Some((ts, committed)) = old {
            debug_assert!(committed, "{id:?} aborted");
            debug_assert_eq!(ts, commit, "{id:?} committed at (different) {ts:?}");
        }
        if old.is_none() {
            inner.txn_log_count += 1;
        }

        inner.do_commit(op_id, id, txn, commit);
    }

    fn remove_prepared_txn(&mut self, id: TransactionId) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let existed = inner.resolve_prepared_txn(&id).is_some();
        if existed {
            inner.do_abort(&id);
        }
        existed
    }

    fn add_or_replace_or_finalize_prepared_txn(
        &mut self,
        op_id: OpId,
        id: TransactionId,
        txn: SharedTransaction<K, V, Timestamp>,
        commit: Timestamp,
        finalized: bool,
    ) {
        let mut inner = self.inner.lock().unwrap();
        if let Some((existing_ts, _)) = inner.resolve_prepared_txn(&id) {
            if existing_ts == commit {
                // Same timestamp — just update finalized flag in the PreparedRef.
                if finalized
                    && let Ok(Some(Some(mut prep_ref))) = inner.prepared.get(&id)
                {
                    prep_ref.finalized = true;
                    inner.prepared.put(id, Some(prep_ref));
                }
                return;
            }
            // Different timestamp — remove old, insert new.
            inner.do_abort(&id);
        }
        let _ = inner.do_prepare(op_id, id, &txn, commit);
        if finalized
            && let Ok(Some(Some(mut prep_ref))) = inner.prepared.get(&id)
        {
            prep_ref.finalized = true;
            inner.prepared.put(id, Some(prep_ref));
        }
    }

    // === Prepared Queries ===

    fn get_prepared_txn(
        &self,
        id: &TransactionId,
    ) -> Option<(Timestamp, SharedTransaction<K, V, Timestamp>, bool)> {
        let inner = self.inner.lock().unwrap();
        let (ts, txn) = inner.resolve_prepared_txn(id)?;
        // Finalized flag is stored in PreparedRef directly (no separate BTreeSet).
        let finalized = matches!(inner.prepared.get(id), Ok(Some(Some(ref r))) if r.finalized);
        Some((ts, txn, finalized))
    }

    fn check_prepare_status(
        &self,
        id: &TransactionId,
        commit: &Timestamp,
    ) -> CheckPrepareStatus {
        let inner = self.inner.lock().unwrap();

        // Cascading lookup: txn_log → prepared → min_prepare_time.
        if let Some((ts, committed)) = txn_log_get_inner(&inner, id) {
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

        if let Some((existing_ts, _)) = inner.resolve_prepared_txn(id)
            && existing_ts == *commit
        {
            let finalized =
                matches!(inner.prepared.get(id), Ok(Some(Some(ref r))) if r.finalized);
            return CheckPrepareStatus::PreparedAtTimestamp { finalized };
        }

        let mpt = inner.min_prepare_times.min_prepare_time();
        if commit.time < mpt
            || inner
                .resolve_prepared_txn(id)
                .map(|(ts, _)| ts.time < mpt)
                .unwrap_or(false)
        {
            return CheckPrepareStatus::TooLate;
        }

        CheckPrepareStatus::Unknown
    }

    fn finalize_prepared_txn(&mut self, id: &TransactionId, commit: &Timestamp) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if let Some((ts, _)) = inner.resolve_prepared_txn(id)
            && ts == *commit
        {
            if let Ok(Some(Some(mut prep_ref))) = inner.prepared.get(id) {
                prep_ref.finalized = true;
                inner.prepared.put(*id, Some(prep_ref));
            }
            return true;
        }
        false
    }

    fn prepared_count(&self) -> usize {
        self.inner.lock().unwrap().prepared_count_inner()
    }

    fn get_oldest_prepared_txn(
        &self,
    ) -> Option<(TransactionId, Timestamp, SharedTransaction<K, V, Timestamp>)> {
        let inner = self.inner.lock().unwrap();
        inner
            .all_prepared_keys()
            .iter()
            .filter_map(|k| {
                let (ts, txn) = inner.resolve_prepared_txn(k)?;
                Some((*k, ts, txn))
            })
            .min_by_key(|(_, ts, _)| *ts)
    }

    fn remove_all_unfinalized_prepared_txns(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        let keys = inner.all_prepared_keys();
        let to_remove: Vec<TransactionId> = keys
            .iter()
            .filter(|k| {
                matches!(inner.prepared.get(k), Ok(Some(Some(ref r))) if !r.finalized)
            })
            .copied()
            .collect();
        for id in to_remove {
            inner.do_abort(&id);
        }
    }

    // === Committed Read/Scan ===

    fn do_committed_get(
        &mut self,
        key: K,
        ts: Timestamp,
    ) -> Result<(Option<V>, Timestamp), PrepareConflict> {
        self.inner.lock().unwrap().snapshot_get_protected(&key, ts)
    }

    fn do_committed_scan(
        &mut self,
        start: K,
        end: K,
        ts: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, PrepareConflict> {
        self.inner
            .lock()
            .unwrap()
            .snapshot_scan_protected(&start, &end, ts)
    }

    // === Uncommitted Validated Reads ===

    fn do_uncommitted_get_validated(
        &self,
        key: &K,
        ts: Timestamp,
    ) -> Option<(Option<V>, Timestamp)> {
        let inner = self.inner.lock().unwrap();
        // Ghost filter: clamp ts so fast path doesn't return ghost entries.
        let ts = inner.effective_snapshot_ts(ts);
        let mvcc_view = MvccView { mvcc: &inner.mvcc };
        let read_ts = mvcc_view.get_last_read_ts_at(key, ts).ok()??;
        if read_ts >= ts {
            let (value, write_ts) = inner.snapshot_get_at(key, ts).ok()?;
            eprintln!("[FP-HIT] key={key:?} snapshot_ts={ts:?} last_read_ts={read_ts:?} write_ts={write_ts:?} value={value:?}");
            Some((value, write_ts))
        } else {
            eprintln!("[FP-MISS] key={key:?} snapshot_ts={ts:?} last_read_ts={read_ts:?} (too old)");
            None
        }
    }

    fn do_uncommitted_scan_validated(
        &self,
        start: &K,
        end: &K,
        ts: Timestamp,
    ) -> Option<Vec<(K, Option<V>, Timestamp)>> {
        let inner = self.inner.lock().unwrap();
        if !inner.occ_cache.has_covering_range_read(start, end, ts) {
            return None;
        }
        inner.snapshot_scan(start, end, ts).ok()
    }

    // === Transaction Log ===

    fn txn_log_get(&self, id: &TransactionId) -> Option<(Timestamp, bool)> {
        txn_log_get_inner(&self.inner.lock().unwrap(), id)
    }

    fn txn_log_insert(
        &mut self,
        id: TransactionId,
        ts: Timestamp,
        committed: bool,
    ) -> Option<(Timestamp, bool)> {
        let mut inner = self.inner.lock().unwrap();
        let old = txn_log_get_inner(&inner, &id);
        if committed {
            // For standalone txn_log_insert(committed=true) without a real IR entry,
            // we store a synthetic op_id. In practice, commit_txn() supplies the real
            // op_id via do_commit(). This path is only used by conformance tests.
            inner.committed.put(
                id,
                TxnLogRef::Committed {
                    op_id: OpId {
                        client_id: crate::IrClientId(0),
                        number: 0,
                    },
                    commit: ts,
                },
            );
        } else {
            inner.committed.put(id, TxnLogRef::Aborted(ts));
        }
        if old.is_none() {
            inner.txn_log_count += 1;
        }
        old
    }

    fn txn_log_contains(&self, id: &TransactionId) -> bool {
        matches!(self.inner.lock().unwrap().committed.get(id), Ok(Some(_)))
    }

    fn txn_log_len(&self) -> usize {
        self.inner.lock().unwrap().txn_log_count
    }

    // === Min Prepare Time ===

    fn raise_min_prepare_time(&mut self, time: u64) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        let min_prepared_ts = inner.min_prepared_timestamp();
        inner.min_prepare_times.raise(time, min_prepared_ts)
    }

    fn finalize_min_prepare_time(&mut self, time: u64) {
        self.inner.lock().unwrap().min_prepare_times.finalize(time);
    }

    fn sync_min_prepare_time(&mut self, time: u64) {
        self.inner.lock().unwrap().min_prepare_times.sync(time);
    }

    fn reset_min_prepare_time_to_finalized(&mut self) {
        self.inner
            .lock()
            .unwrap()
            .min_prepare_times
            .reset_to_finalized();
    }

    // === CDC Deltas ===

    fn record_cdc_delta(&mut self, base_view: u64, delta: LeaderRecordDelta<K, V>) {
        self.inner
            .lock()
            .unwrap()
            .record_delta_during_view
            .record_cdc_delta(base_view, delta);
    }

    fn cdc_deltas_from(&self, from_view: u64) -> Vec<LeaderRecordDelta<K, V>> {
        self.inner
            .lock()
            .unwrap()
            .record_delta_during_view
            .cdc_deltas_from(from_view)
    }

    fn cdc_max_view(&self) -> Option<u64> {
        self.inner
            .lock()
            .unwrap()
            .record_delta_during_view
            .cdc_max_view()
    }

    // === Resharding ===

    fn min_prepare_baseline(&self) -> Option<Timestamp> {
        self.inner.lock().unwrap().occ_cache.max_read_time()
    }

    fn flush(&mut self) {
        self.inner
            .lock()
            .unwrap()
            .seal_tapir_side()
            .expect("CombinedTapirHandle::flush: seal failed");
    }

    fn stored_bytes(&self) -> Option<u64> {
        Some(self.inner.lock().unwrap().tapir_stored_bytes())
    }
}

// ---------------------------------------------------------------------------
// Helper: read TxnLogRef from committed VlogLsm and resolve to (Timestamp, bool).
// The commit timestamp is stored directly in TxnLogRef::Committed, avoiding
// lazy resolution from inc_lsm which can fail after view-change seals drop
// tentative IR entries.
// ---------------------------------------------------------------------------

fn txn_log_get_inner<K: Ord, V, DIO: DiskIo>(
    inner: &CombinedStoreInner<K, V, DIO>,
    id: &TransactionId,
) -> Option<(Timestamp, bool)> {
    match inner.committed.get(id) {
        Ok(Some(TxnLogRef::Committed { commit, .. })) => Some((commit, true)),
        Ok(Some(TxnLogRef::Aborted(ts))) => Some((ts, false)),
        _ => None,
    }
}
