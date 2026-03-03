pub mod cli;
mod ir;
mod tapir;
mod tapir_recovery;
mod wisckeylsm;
pub mod types;

use crate::ir::OpId;
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use crate::occ::{Transaction, TransactionId as OccTransactionId};
use crate::tapir::{ShardNumber, Timestamp};
use crate::tapirstore::{CheckPrepareStatus, TapirStore};
use std::path::{Path, PathBuf};

#[cfg(test)]
use crate::occ::Timestamp as _;

use ir::record::{IrPayloadInline, IrRecord, VlogEntryType};
use tapir::CachedPrepare;
use types::*;
use wisckeylsm::vlog::UnifiedVlogSegment;

/// Default minimum VLog segment size before starting a new file (256 KB).
const DEFAULT_MIN_VIEW_VLOG_SIZE: u64 = 256 * 1024;

/// The unified storage engine backing both IR record persistence
/// and MVCC value storage through a shared VLog.
///
/// # Data lifecycle
///
/// `K` and `V` are stored **typed** in memory (`ir_overlay`,
/// `prepare_registry`) so that prepared transactions can be inspected,
/// indexed, and committed without a serialization round-trip.
/// Serialization to bytes happens exactly once at `seal_current_view()`
/// time when the data is written to the VLog.  Deserialization from the
/// VLog happens only for cross-view reads (rare), and the result is
/// cached in `prepare_cache` to avoid repeated VLog I/O.
///
/// Because of this, `K` and `V` must be `Send + Sync` (required by
/// `Arc<CachedPrepare<K, V>>` which must be `Send` for the `MvccBackend`
/// trait), and `Serialize + DeserializeOwned` (for VLog persistence).
///
/// # Value indirection
///
/// Committed values are NOT duplicated in the MVCC index.  Instead,
/// each MVCC entry (`UnifiedLsmEntry`) holds a `ValueLocation` that
/// points either to the `prepare_registry` (legacy/current-view path)
/// or to a TAPIR committed-transaction entry in the VLog. Reading a
/// committed value always goes through `resolve_in_memory` or
/// `resolve_on_disk`.
pub struct UnifiedStore<K: Ord, V, IO: DiskIo> {
    ir_state: IrRecord<K, V, IO>,
    tapir_state: tapir::store::TapirState<K, V, IO>,

    /// Base directory for all on-disk files.
    base_dir: PathBuf,

    /// I/O flags.
    io_flags: OpenFlags,

    /// Minimum VLog segment size before starting a new file.
    min_view_vlog_size: u64,

    /// Number of entries written to the current view's VLog (for view seal).
    current_view_entry_count: u32,
}

impl<K: Ord + Clone, V, IO: DiskIo> UnifiedStore<K, V, IO> {
    /// Open or create a unified store at the given directory.
    pub fn open(base_dir: PathBuf) -> Result<Self, StorageError>
    where
        K: crate::tapir::Key,
        V: crate::tapir::Value,
    {
        Self::open_with_options(base_dir, DEFAULT_MIN_VIEW_VLOG_SIZE)
    }

    /// Open with custom minimum VLog segment size.
    pub fn open_with_options(
        base_dir: PathBuf,
        min_view_vlog_size: u64,
    ) -> Result<Self, StorageError>
    where
        K: crate::tapir::Key,
        V: crate::tapir::Value,
    {
        let io_flags = OpenFlags {
            create: true,
            direct: false,
        };

        IO::create_dir_all(&base_dir)?;
        let ir_state = ir::store::open_store_state::<K, V, IO>(&base_dir, io_flags)?;
        let tapir_state = tapir::store::open::<K, V, IO>(&base_dir.join("tapir"), io_flags)?;

        Ok(Self {
            ir_state,
            tapir_state,
            base_dir,
            io_flags,
            min_view_vlog_size,
            current_view_entry_count: 0,
        })
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn current_view(&self) -> u64 {
        self.ir_state.current_view()
    }

    pub fn vlog_read_count(&self) -> u64 {
        self.tapir_state.vlog_read_count()
    }

    pub fn sealed_vlog_segments(&self) -> &std::collections::BTreeMap<u64, UnifiedVlogSegment<IO>> {
        self.ir_state.sealed_vlog_segments()
    }

    /// Get the active VLog segment's ID.
    pub fn active_vlog_id(&self) -> u64 {
        self.ir_state.active_vlog_id()
    }

    /// Get the active VLog segment's write offset (bytes written).
    pub fn active_vlog_write_offset(&self) -> u64 {
        self.ir_state.active_vlog_write_offset()
    }

    /// Get the active VLog segment's view ranges.
    pub fn active_vlog_views(&self) -> &[ViewRange] {
        self.ir_state.active_vlog_views()
    }

    /// Dump all entries from a VLog segment (by segment ID).
    /// Returns entries from either the active or a sealed segment.
    pub fn dump_vlog_segment(
        &self,
        segment_id: u64,
    ) -> Result<Vec<(u64, crate::ir::OpId, VlogEntryType, IrPayloadInline<K, V>)>, StorageError>
    where
        K: serde::de::DeserializeOwned,
        V: serde::de::DeserializeOwned,
    {
        if let Some(seg) = self.ir_state.active_or_sealed_segment(segment_id) {
            seg.iter_entries()
        } else {
            Err(StorageError::Codec(format!(
                "VLog segment {segment_id} not found"
            )))
        }
    }

    /// Dump committed TAPIR transaction entries from a VLog segment.
    pub fn dump_tapir_vlog_segment(
        &self,
        segment_id: u64,
    ) -> Result<Vec<(u64, CachedPrepare<K, V>)>, StorageError>
    where
        K: serde::de::DeserializeOwned,
        V: serde::de::DeserializeOwned,
    {
        if let Some(seg) = self.ir_state.active_or_sealed_segment(segment_id) {
            seg.iter_committed_txn_entries()
        } else {
            Err(StorageError::Codec(format!(
                "VLog segment {segment_id} not found"
            )))
        }
    }

    /// Iterate over all IR overlay entries.
    pub fn ir_overlay_entries(&self) -> impl Iterator<Item = (&OpId, &IrMemEntry<K, V>)> {
        ir::store::ir_overlay_entries(&self.ir_state)
    }

    /// Insert an IR entry into the overlay.
    pub fn insert_ir_entry(&mut self, op_id: OpId, entry: IrMemEntry<K, V>) {
        ir::store::insert_ir_entry(&mut self.ir_state, op_id, entry);
    }

    /// Look up an IR entry by OpId (overlay first, then base).
    pub fn ir_entry(&self, op_id: &OpId) -> Option<IrEntryRef<'_, K, V>> {
        ir::store::ir_entry(&self.ir_state, op_id)
    }

    /// Look up an IR base SST entry by OpId.
    pub fn lookup_ir_base_entry(&self, op_id: OpId) -> Option<&IrSstEntry> {
        ir::store::lookup_ir_base_entry(&self.ir_state, op_id)
    }

    /// Register a prepared transaction for future zero-copy commit.
    ///
    /// Extracts typed K, V from the `Transaction` and stores them in
    /// the prepare_registry.  Must be called before the corresponding
    /// `commit_prepared` so that committed MVCC entries
    /// can point into the registry via `ValueLocation::InMemory`.
    ///
    /// No serialization happens here — that is deferred to seal time.
    pub fn register_prepare(
        &mut self,
        txn_id: OccTransactionId,
        transaction: &Transaction<K, V, Timestamp>,
        commit_ts: Timestamp,
    )
    where
        K: crate::tapir::Key,
        V: crate::tapir::Value,
        V: Clone,
    {
        tapir::store::register_prepare(&mut self.tapir_state, txn_id, transaction, commit_ts);
    }

    /// Commit a prepared transaction by creating MVCC index entries.
    ///
    /// The `commit` timestamp is the **final** commit timestamp chosen by
    /// the TAPIR coordinator.  It may differ from `CachedPrepare::commit_ts`
    /// (the prepare-time proposal): when replicas return `Retry { proposed }`,
    /// the coordinator picks the maximum as the final timestamp.  MVCC
    /// entries must use this final timestamp so that reads at a given
    /// snapshot see the correct version.
    ///
    /// Two paths, tried in order:
    ///
    /// 1. **Registry-backed commit** — prepare is in current-view
    ///    `prepare_registry`; commit appends a TAPIR committed-transaction
    ///    entry to VLog and MVCC entries point to it.
    ///
    /// 2. **OnDisk** — transaction already exists in the committed
    ///    transaction VLog index (idempotent replay / cross-view finalize).
    ///
    /// Returns `Err(StorageError::PrepareNotFound)` if neither the
    /// `prepare_registry` nor the committed transaction index has the transaction.
    pub fn commit_prepared(
        &mut self,
        txn_id: OccTransactionId,
        commit: Timestamp,
    ) -> Result<(), StorageError>
    where
        K: crate::tapir::Key,
        V: crate::tapir::Value,
        K: serde::Serialize + Clone + serde::de::DeserializeOwned,
        V: serde::Serialize + Clone + serde::de::DeserializeOwned,
    {
        tapir::store::commit_prepared(&mut self.tapir_state, txn_id, commit)
    }

    pub fn commit_transaction_data(
        &mut self,
        txn_id: OccTransactionId,
        read_set: &[(K, Timestamp)],
        write_set: &[(K, Option<V>)],
        scan_set: &[(K, K, Timestamp)],
        commit: Timestamp,
    ) -> Result<(), StorageError>
    where
        K: crate::tapir::Key,
        V: crate::tapir::Value,
        K: serde::Serialize + Clone + serde::de::DeserializeOwned,
        V: serde::Serialize + Clone + serde::de::DeserializeOwned,
    {
        tapir::store::commit_transaction_data(
            &mut self.tapir_state,
            txn_id,
            read_set,
            write_set,
            scan_set,
            commit,
        )
    }

    /// Remove a prepare from the in-memory registry.
    pub fn unregister_prepare(&mut self, txn_id: &OccTransactionId)
    where
        K: crate::tapir::Key,
        V: crate::tapir::Value,
    {
        tapir::store::unregister_prepare(&mut self.tapir_state, txn_id);
    }

    /// Resolve a value from the prepare registry (InMemory path).
    ///
    /// Returns a reference to the typed `(key, Option<value>)` pair
    /// from the current view's `prepare_registry`.  Returns `None` if
    /// the transaction has been sealed or was never registered.
    pub fn resolve_in_memory(
        &self,
        txn_id: &OccTransactionId,
        write_index: u16,
    ) -> Option<&(K, Option<V>)> {
        self.tapir_state.resolve_in_memory_ref(txn_id, write_index)
    }

    /// Resolve a committed value from its `ValueLocation`.
    ///
    /// Both paths return typed `V` without the caller needing to know
    /// whether the value is in memory or on disk:
    ///
    /// - `InMemory` → zero-copy lookup in `prepare_registry` (current view)
    /// - `OnDisk` → VLog read + LRU cache lookup (sealed views)
    /// - `None` → delete tombstone or metadata-only entry (e.g. `commit_get`)
    pub fn resolve_value(&self, entry: &UnifiedLsmEntry) -> Result<Option<V>, StorageError>
    where
        K: crate::tapir::Key,
        V: crate::tapir::Value,
        K: serde::de::DeserializeOwned,
        V: Clone + serde::de::DeserializeOwned,
    {
        tapir::store::resolve_value(&self.tapir_state, entry)
    }

    /// Get a reference to the unified MVCC memtable.
    pub(crate) fn unified_memtable(&self) -> &crate::unified::wisckeylsm::unified_memtable::UnifiedMemtable<K> {
        self.tapir_state.unified_memtable()
    }

    /// Get a mutable reference to the unified MVCC memtable.
    #[cfg(test)]
    pub(crate) fn unified_memtable_mut(&mut self) -> &mut crate::unified::wisckeylsm::unified_memtable::UnifiedMemtable<K> {
        self.tapir_state.unified_memtable_mut()
    }

    /// Seal the current view: serialize typed overlay entries to the VLog,
    /// convert `InMemory` MVCC entries to `OnDisk`, clear overlay and
    /// registry, advance to the next view.
    ///
    /// This is the only point where typed `K, V` are serialized to bytes.
    /// After seal, all value resolution for this view goes through
    /// `resolve_on_disk` (VLog read + `prepare_cache`).
    pub fn seal_current_view(&mut self) -> Result<(), StorageError>
    where
        K: serde::Serialize + Clone,
        V: serde::Serialize + Clone,
    {
        ir::store::seal_current_view(
            &mut self.ir_state,
            &self.base_dir,
            self.io_flags,
            self.min_view_vlog_size,
        )?;
        self.tapir_state.seal_current_view(self.min_view_vlog_size)?;
        ir::store::clear_overlay(&mut self.ir_state);
        self.current_view_entry_count = 0;

        Ok(())
    }

    /// Extract only finalized entries from the overlay (for leader merge simulation).
    pub fn extract_finalized_entries(&self) -> Vec<(OpId, IrMemEntry<K, V>)>
    where
        K: Clone,
        V: Clone,
    {
        ir::store::extract_finalized_entries(&self.ir_state)
    }

    /// Install a merged record as the new IR base.
    /// All entries are marked Finalized(target_view).
    pub fn install_merged_record(
        &mut self,
        entries: Vec<(OpId, IrMemEntry<K, V>)>,
        target_view: u64,
    ) -> Result<(), StorageError>
    where
        K: serde::Serialize,
        V: serde::Serialize,
    {
        // Write entries to VLog
        let entry_refs: Vec<(OpId, VlogEntryType, &IrPayloadInline<K, V>)> = entries
            .iter()
            .map(|(op, entry)| (*op, entry.entry_type, &entry.payload))
            .collect();

        let ptrs = if !entry_refs.is_empty() {
            self.ir_state.append_batch_to_active(&entry_refs)?
        } else {
            Vec::new()
        };

        // Install new IR base from VLog pointers (clears base + overlay)
        ir::store::install_base_from_ptrs(&mut self.ir_state, &entries, &ptrs);

        self.ir_state.set_current_view_for_install(target_view);
        self.tapir_state.clear_prepare_registry();
        // Note: unified_memtable is NOT cleared — committed entries from
        // previous views remain valid with OnDisk pointers.

        Ok(())
    }

    pub(crate) fn get_range(
        &self,
        key: &K,
        timestamp: Timestamp,
    ) -> Result<(Timestamp, Option<Timestamp>), StorageError> {
        if let Some((ck, _entry)) = self.unified_memtable().get_at(key, timestamp) {
            let write_ts = ck.timestamp.0;
            let next = self.unified_memtable().find_next_version(key, write_ts);
            return Ok((write_ts, next));
        }
        Ok((Timestamp::default(), None))
    }

    #[cfg(test)]
    pub(crate) fn commit_get(
        &mut self,
        key: K,
        read: Timestamp,
        commit: Timestamp,
    ) -> Result<(), StorageError> {
        self.unified_memtable_mut()
            .update_last_read(&key, read, commit.time);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn get_last_read(&self, key: &K) -> Result<Option<Timestamp>, StorageError> {
        if let Some((_, entry)) = self.unified_memtable().get_latest(key)
            && let Some(ts) = entry.last_read_ts
        {
            return Ok(Some(Timestamp::from_time(ts)));
        }
        Ok(None)
    }

    #[cfg(test)]
    pub(crate) fn get_last_read_at(
        &self,
        key: &K,
        timestamp: Timestamp,
    ) -> Result<Option<Timestamp>, StorageError> {
        if let Some((_, entry)) = self.unified_memtable().get_at(key, timestamp)
            && let Some(ts) = entry.last_read_ts
        {
            return Ok(Some(Timestamp::from_time(ts)));
        }
        Ok(None)
    }

    pub(crate) fn has_writes_in_range(
        &self,
        start: &K,
        end: &K,
        after_ts: Timestamp,
        before_ts: Timestamp,
    ) -> Result<bool, StorageError> {
        Ok(self
            .unified_memtable()
            .has_writes_in_range(start, end, after_ts, before_ts))
    }
}

impl<K: crate::tapir::Key, V: crate::tapir::Value, IO: DiskIo> TapirStore<K, V>
    for UnifiedStore<K, V, IO>
{
    fn shard(&self) -> ShardNumber {
        todo!()
    }

    fn do_uncommitted_get(&self, key: &K) -> Result<(Option<V>, Timestamp), StorageError> {
        tapir::store::do_uncommitted_get(&self.tapir_state, key)
    }

    fn do_uncommitted_get_at(&self, key: &K, ts: Timestamp) -> Result<(Option<V>, Timestamp), StorageError> {
        tapir::store::do_uncommitted_get_at(&self.tapir_state, key, ts)
    }

    fn do_uncommitted_scan(
        &self,
        start: &K,
        end: &K,
        ts: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, StorageError> {
        tapir::store::do_uncommitted_scan(&self.tapir_state, start, end, ts)
    }

    fn try_prepare_txn(
        &mut self,
        _id: OccTransactionId,
        _txn: crate::occ::SharedTransaction<K, V, Timestamp>,
        _commit: Timestamp,
    ) -> crate::occ::PrepareResult<Timestamp> {
        todo!()
    }

    fn commit_txn(
        &mut self,
        _id: OccTransactionId,
        _txn: &Transaction<K, V, Timestamp>,
        _commit: Timestamp,
    ) {
        todo!()
    }

    fn remove_prepared_txn(&mut self, _id: OccTransactionId) -> bool {
        todo!()
    }

    fn add_or_replace_or_finalize_prepared_txn(
        &mut self,
        _id: OccTransactionId,
        _txn: crate::occ::SharedTransaction<K, V, Timestamp>,
        _commit: Timestamp,
        _finalized: bool,
    ) {
        todo!()
    }

    fn get_prepared_txn(
        &self,
        _id: &OccTransactionId,
    ) -> Option<(&Timestamp, &crate::occ::SharedTransaction<K, V, Timestamp>, bool)> {
        todo!()
    }

    fn check_prepare_status(
        &self,
        _id: &OccTransactionId,
        _commit: &Timestamp,
    ) -> CheckPrepareStatus {
        todo!()
    }

    fn finalize_prepared_txn(&mut self, _id: &OccTransactionId, _commit: &Timestamp) -> bool {
        todo!()
    }

    fn prepared_count(&self) -> usize {
        todo!()
    }

    fn get_oldest_prepared_txn(
        &self,
    ) -> Option<(OccTransactionId, Timestamp, crate::occ::SharedTransaction<K, V, Timestamp>)> {
        todo!()
    }

    fn remove_all_unfinalized_prepared_txns(&mut self) {
        todo!()
    }

    fn do_committed_get(
        &mut self,
        _key: K,
        _ts: Timestamp,
    ) -> Result<(Option<V>, Timestamp), crate::occ::PrepareConflict> {
        todo!()
    }

    fn do_committed_scan(
        &mut self,
        _start: K,
        _end: K,
        _ts: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, crate::occ::PrepareConflict> {
        todo!()
    }

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

    fn txn_log_get(&self, id: &OccTransactionId) -> Option<(Timestamp, bool)> {
        tapir::store::txn_log_get(&self.tapir_state, id)
    }

    fn txn_log_insert(
        &mut self,
        id: OccTransactionId,
        ts: Timestamp,
        committed: bool,
    ) -> Option<(Timestamp, bool)> {
        tapir::store::txn_log_insert(&mut self.tapir_state, id, ts, committed)
    }

    fn txn_log_contains(&self, id: &OccTransactionId) -> bool {
        tapir::store::txn_log_contains(&self.tapir_state, id)
    }

    fn txn_log_len(&self) -> usize {
        tapir::store::txn_log_len(&self.tapir_state)
    }

    fn raise_min_prepare_time(&mut self, time: u64) -> u64 {
        tapir::store::raise_min_prepare_time(&mut self.tapir_state, time)
    }

    fn finalize_min_prepare_time(&mut self, time: u64) {
        tapir::store::finalize_min_prepare_time(&mut self.tapir_state, time)
    }

    fn sync_min_prepare_time(&mut self, time: u64) {
        tapir::store::sync_min_prepare_time(&mut self.tapir_state, time)
    }

    fn reset_min_prepare_time_to_finalized(&mut self) {
        tapir::store::reset_min_prepare_time_to_finalized(&mut self.tapir_state)
    }

    fn record_cdc_delta(&mut self, base_view: u64, delta: crate::tapir::LeaderRecordDelta<K, V>) {
        tapir::store::record_cdc_delta(&mut self.tapir_state, base_view, delta)
    }

    fn cdc_deltas_from(&self, from_view: u64) -> Vec<crate::tapir::LeaderRecordDelta<K, V>> {
        tapir::store::cdc_deltas_from(&self.tapir_state, from_view)
    }

    fn cdc_max_view(&self) -> Option<u64> {
        tapir::store::cdc_max_view(&self.tapir_state)
    }

    fn min_prepare_baseline(&self) -> Option<Timestamp> {
        todo!()
    }
}

