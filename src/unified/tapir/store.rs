use crate::mvcc::disk::error::StorageError;
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::occ::TransactionId as OccTransactionId;
#[cfg(test)]
use crate::occ::TransactionId;
use crate::tapir::{Key, Timestamp, Value};
#[cfg(test)]
use crate::tapir::LeaderRecordDelta;
#[cfg(test)]
use crate::tapirstore::{MinPrepareTimes, RecordDeltaDuringView, TransactionLog};
use crate::unified::tapir::CachedPrepare;
use crate::unified::tapir::{LsmEntry, ValueLocation, VlogSegment, VlogTransactionPtr};
#[cfg(test)]
use crate::IrClientId;
use crate::unified::tapir::unified_memtable::Memtable;
use crate::unified::wisckeylsm::manifest::UnifiedManifest;
use crate::unified::tapir::prepare_cache::PreparedTransactions;
use crate::unified::wisckeylsm::types::{VlogPtr, VlogSegmentMeta};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[allow(dead_code)]
pub(crate) struct TapirState<K: Ord, V, IO: DiskIo> {
    unified_memtable: Memtable<K>,
    prepare_registry: BTreeMap<OccTransactionId, Arc<CachedPrepare<K, V>>>,
    prepare_cache: RefCell<PreparedTransactions<CachedPrepare<K, V>>>,
    committed_txn_vlog_index: BTreeMap<OccTransactionId, VlogPtr>,
    vlog_read_count: Cell<u64>,
    #[cfg(test)]
    txlog: TransactionLog,
    #[cfg(test)]
    min_prepare: MinPrepareTimes,
    #[cfg(test)]
    cdc: RecordDeltaDuringView<K, V>,
    active_vlog: VlogSegment<IO>,
    sealed_vlog_segments: BTreeMap<u64, VlogSegment<IO>>,
    manifest: UnifiedManifest,
    base_dir: PathBuf,
    io_flags: OpenFlags,
    current_view_entry_count: u32,
}

fn new_store_state<K: Ord + Clone, V, IO: DiskIo>(
    prepare_cache_capacity: usize,
    active_vlog: VlogSegment<IO>,
    sealed_vlog_segments: BTreeMap<u64, VlogSegment<IO>>,
    manifest: UnifiedManifest,
    base_dir: PathBuf,
    io_flags: OpenFlags,
) -> TapirState<K, V, IO> {
    TapirState {
        unified_memtable: Memtable::new(),
        prepare_registry: BTreeMap::new(),
        prepare_cache: RefCell::new(PreparedTransactions::new(prepare_cache_capacity)),
        committed_txn_vlog_index: BTreeMap::new(),
        vlog_read_count: Cell::new(0),
        #[cfg(test)]
        txlog: TransactionLog::new(),
        #[cfg(test)]
        min_prepare: MinPrepareTimes::new(),
        #[cfg(test)]
        cdc: RecordDeltaDuringView::new(),
        active_vlog,
        sealed_vlog_segments,
        manifest,
        base_dir,
        io_flags,
        current_view_entry_count: 0,
    }
}

#[allow(dead_code)]
impl<K: Ord + Clone, V, IO: DiskIo> TapirState<K, V, IO> {
    pub(crate) fn vlog_read_count(&self) -> u64 {
        self.vlog_read_count.get()
    }

    pub(crate) fn increment_vlog_read_count(&self) {
        self.vlog_read_count.set(self.vlog_read_count.get() + 1);
    }

    pub(crate) fn unified_memtable(&self) -> &Memtable<K> {
        &self.unified_memtable
    }

    pub(crate) fn unified_memtable_mut(&mut self) -> &mut Memtable<K> {
        &mut self.unified_memtable
    }

    pub(crate) fn register_prepare_raw(
        &mut self,
        txn_id: OccTransactionId,
        prepare: Arc<CachedPrepare<K, V>>,
    ) {
        self.prepare_registry.insert(txn_id, prepare);
    }

    #[cfg(test)]
    pub(crate) fn unregister_prepare(&mut self, txn_id: &OccTransactionId) {
        self.prepare_registry.remove(txn_id);
    }

    #[cfg(test)]
    pub(crate) fn lookup_prepare(&self, txn_id: &OccTransactionId) -> Option<Arc<CachedPrepare<K, V>>> {
        self.prepare_registry.get(txn_id).cloned()
    }

    pub(crate) fn lookup_prepare_value_ref(
        &self,
        txn_id: &OccTransactionId,
        write_index: u16,
    ) -> Option<(K, Option<V>)>
    where
        K: Clone,
        V: Clone,
    {
        self.prepare_registry
            .get(txn_id)
            .and_then(|p| p.write_set.get(write_index as usize))
            .cloned()
    }

    #[cfg(test)]
    pub(crate) fn resolve_in_memory_ref(
        &self,
        txn_id: &OccTransactionId,
        write_index: u16,
    ) -> Option<&(K, Option<V>)> {
        self.prepare_registry
            .get(txn_id)
            .and_then(|p| p.write_set.get(write_index as usize))
    }

    #[cfg(test)]
    pub(crate) fn has_prepare_write_conflict(&self, key: &K) -> bool {
        self.prepare_registry
            .values()
            .flat_map(|p| p.write_set.iter())
            .any(|(k, _)| k == key)
    }

    #[cfg(test)]
    pub(crate) fn contains_prepare(&self, txn_id: &OccTransactionId) -> bool {
        self.prepare_registry.contains_key(txn_id)
    }

    pub(crate) fn clear_prepare_registry(&mut self) {
        self.prepare_registry.clear();
    }

    #[cfg(test)]
    pub(crate) fn min_prepared_commit_time(&self) -> Option<u64> {
        self.prepare_registry
            .values()
            .map(|p| p.commit_ts.time)
            .min()
    }

    pub(crate) fn index_committed_txn_ptr(&mut self, txn_id: OccTransactionId, ptr: VlogPtr) {
        self.committed_txn_vlog_index.insert(txn_id, ptr);
    }

    #[cfg(test)]
    pub(crate) fn lookup_committed_txn_ptr(&self, txn_id: &OccTransactionId) -> Option<VlogPtr> {
        self.committed_txn_vlog_index.get(txn_id).copied()
    }

    pub(crate) fn prepare_cache_get(
        &self,
        segment_id: u64,
        offset: u64,
    ) -> Option<Arc<CachedPrepare<K, V>>> {
        self.prepare_cache.borrow_mut().get(segment_id, offset)
    }

    pub(crate) fn prepare_cache_insert(
        &self,
        segment_id: u64,
        offset: u64,
        prepare: Arc<CachedPrepare<K, V>>,
    ) {
        self.prepare_cache
            .borrow_mut()
            .insert(segment_id, offset, prepare);
    }

    #[cfg(test)]
    pub(crate) fn txn_log_get(&self, id: &OccTransactionId) -> Option<(Timestamp, bool)> {
        self.txlog.txn_log_get(id)
    }

    #[cfg(test)]
    pub(crate) fn txn_log_insert(
        &mut self,
        id: OccTransactionId,
        ts: Timestamp,
        committed: bool,
    ) -> Option<(Timestamp, bool)> {
        self.txlog.txn_log_insert(id, ts, committed)
    }

    #[cfg(test)]
    pub(crate) fn txn_log_contains(&self, id: &OccTransactionId) -> bool {
        self.txlog.txn_log_contains(id)
    }

    #[cfg(test)]
    pub(crate) fn txn_log_len(&self) -> usize {
        self.txlog.txn_log_len()
    }

    #[cfg(test)]
    pub(crate) fn raise_min_prepare_time(&mut self, time: u64) -> u64 {
        self.min_prepare.raise(time, self.min_prepared_commit_time())
    }

    #[cfg(test)]
    pub(crate) fn finalize_min_prepare_time(&mut self, time: u64) {
        self.min_prepare.finalize(time)
    }

    #[cfg(test)]
    pub(crate) fn sync_min_prepare_time(&mut self, time: u64) {
        self.min_prepare.sync(time)
    }

    #[cfg(test)]
    pub(crate) fn reset_min_prepare_time_to_finalized(&mut self) {
        self.min_prepare.reset_to_finalized()
    }

    #[cfg(test)]
    pub(crate) fn record_cdc_delta(&mut self, base_view: u64, delta: LeaderRecordDelta<K, V>) {
        self.cdc.record_cdc_delta(base_view, delta);
    }

    #[cfg(test)]
    pub(crate) fn cdc_deltas_from(&self, from_view: u64) -> Vec<LeaderRecordDelta<K, V>>
    where
        V: Clone,
    {
        self.cdc.cdc_deltas_from(from_view)
    }

    #[cfg(test)]
    pub(crate) fn cdc_max_view(&self) -> Option<u64> {
        self.cdc.cdc_max_view()
    }

    #[cfg(test)]
    pub(crate) fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub(crate) fn current_view(&self) -> u64 {
        self.manifest.current_view
    }

    pub(crate) fn sealed_vlog_segments(&self) -> &BTreeMap<u64, VlogSegment<IO>> {
        &self.sealed_vlog_segments
    }

    pub(crate) fn active_vlog_id(&self) -> u64 {
        self.active_vlog.id
    }

    pub(crate) fn active_or_sealed_segment_ref(&self, segment_id: u64) -> Option<&VlogSegment<IO>> {
        self.active_or_sealed_segment(segment_id)
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

    #[cfg(test)]
    pub(crate) fn resolve_in_memory(
        &self,
        txn_id: &OccTransactionId,
        write_index: u16,
    ) -> Option<&(K, Option<V>)> {
        self.resolve_in_memory_ref(txn_id, write_index)
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
            return Ok(Some(Timestamp {
                time: ts,
                client_id: IrClientId(1),
            }));
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
            return Ok(Some(Timestamp {
                time: ts,
                client_id: IrClientId(1),
            }));
        }
        Ok(None)
    }

    #[cfg(test)]
    pub(crate) fn resolve_value(
        &self,
        entry: &LsmEntry,
    ) -> Result<Option<V>, StorageError>
    where
        K: Key + serde::Serialize + serde::de::DeserializeOwned,
        V: Value + serde::Serialize + serde::de::DeserializeOwned,
    {
        resolve_value(self, entry)
    }

    #[cfg(test)]
    pub(crate) fn commit_transaction_data(
        &mut self,
        txn_id: OccTransactionId,
        read_set: &[(K, Timestamp)],
        write_set: &[(K, Option<V>)],
        scan_set: &[(K, K, Timestamp)],
        commit: Timestamp,
    ) -> Result<(), StorageError>
    where
        K: Key,
        V: Value,
    {
        commit_transaction_data(self, txn_id, read_set, write_set, scan_set, commit)
    }

    #[cfg(test)]
    pub(crate) fn commit_prepared(
        &mut self,
        txn_id: OccTransactionId,
        commit: Timestamp,
    ) -> Result<(), StorageError>
    where
        K: Key,
        V: Value,
    {
        commit_prepared(self, txn_id, commit)
    }

    #[cfg(test)]
    pub(crate) fn register_prepare(
        &mut self,
        txn_id: OccTransactionId,
        transaction: &crate::occ::Transaction<K, V, Timestamp>,
        commit_ts: Timestamp,
    )
    where
        K: Key,
        V: Value,
    {
        register_prepare(self, txn_id, transaction, commit_ts)
    }

    #[cfg(test)]
    pub(crate) fn unregister_prepare_entry(&mut self, txn_id: &OccTransactionId) {
        self.unregister_prepare(txn_id)
    }

    pub(crate) fn convert_in_memory_to_on_disk(&mut self) {
        let committed_index = self.committed_txn_vlog_index.clone();
        self.unified_memtable
            .convert_in_memory_to_on_disk(&committed_index);
    }

    fn active_or_sealed_segment(&self, segment_id: u64) -> Option<&VlogSegment<IO>> {
        self.sealed_vlog_segments
            .get(&segment_id)
            .or(if self.active_vlog.id == segment_id {
                Some(&self.active_vlog)
            } else {
                None
            })
    }

    fn append_committed_txn_to_active(
        &mut self,
        txn_id: OccTransactionId,
        commit: Timestamp,
        read_set: &[(K, Timestamp)],
        write_set: &[(K, Option<V>)],
        scan_set: &[(K, K, Timestamp)],
    ) -> Result<VlogPtr, StorageError>
    where
        K: serde::Serialize,
        V: serde::Serialize,
    {
        let ptr = append_committed_txn_to_segment(
            &mut self.active_vlog,
            txn_id,
            commit,
            read_set,
            write_set,
            scan_set,
        )?;
        self.current_view_entry_count += 1;
        Ok(ptr)
    }

    pub(crate) fn seal_current_view(&mut self, min_view_vlog_size: u64) -> Result<(), StorageError> {
        self.active_vlog.sync()?;
        self.active_vlog
            .finish_view(self.current_view_entry_count);

        let segment_size = self.active_vlog.write_offset();
        if segment_size >= min_view_vlog_size {
            let sealed_id = self.active_vlog.id;
            let sealed_path = self.active_vlog.path().clone();
            let sealed_views = self.active_vlog.views.clone();
            let sealed_size = self.active_vlog.write_offset();

            self.manifest.sealed_vlog_segments.push(VlogSegmentMeta {
                segment_id: sealed_id,
                path: sealed_path.clone(),
                views: sealed_views.clone(),
                total_size: sealed_size,
            });

            let old_active = std::mem::replace(
                &mut self.active_vlog,
                {
                    let new_id = self.manifest.next_segment_id;
                    self.manifest.next_segment_id += 1;
                    let new_path = self
                        .base_dir
                        .join(format!("vlog_seg_{new_id:04}.dat"));
                    VlogSegment::<IO>::open(new_id, new_path, self.io_flags)?
                },
            );
            self.sealed_vlog_segments.insert(
                sealed_id,
                VlogSegment::<IO>::open_at(
                    sealed_id,
                    sealed_path,
                    sealed_size,
                    sealed_views,
                    self.io_flags,
                )?,
            );
            old_active.close();
        }

        self.manifest.current_view += 1;
        self.manifest.active_segment_id = self.active_vlog.id;
        self.manifest.active_write_offset = self.active_vlog.write_offset();
        self.manifest.save::<IO>(&self.base_dir)?;
        self.active_vlog.start_view(self.manifest.current_view);

        finalize_after_seal(self);
        self.current_view_entry_count = 0;
        Ok(())
    }
}

const DEFAULT_PREPARE_CACHE_CAPACITY: usize = 1024;
const RAW_ENTRY_OVERHEAD: u32 = 25;
const TAPIR_COMMITTED_TXN_ENTRY_TYPE: u8 = 0x80;

fn append_committed_txn_to_segment<K: serde::Serialize, V: serde::Serialize, IO: DiskIo>(
    segment: &mut VlogSegment<IO>,
    txn_id: OccTransactionId,
    commit: Timestamp,
    read_set: &[(K, Timestamp)],
    write_set: &[(K, Option<V>)],
    scan_set: &[(K, K, Timestamp)],
) -> Result<VlogPtr, StorageError> {
    let payload = crate::unified::tapir::vlog_codec::serialize_committed_txn_payload(
        txn_id,
        commit,
        read_set,
        write_set,
        scan_set,
    )?;
    segment.append_raw_entry(
        TAPIR_COMMITTED_TXN_ENTRY_TYPE,
        txn_id.client_id.0,
        txn_id.number,
        &payload,
    )
}

fn read_committed_txn_from_segment<
    K: serde::de::DeserializeOwned,
    V: serde::de::DeserializeOwned,
    IO: DiskIo,
>(
    segment: &VlogSegment<IO>,
    txn_ptr: &VlogPtr,
) -> Result<CachedPrepare<K, V>, StorageError> {
    let raw = segment.read_raw_entry(txn_ptr)?;
    if raw.entry_type != TAPIR_COMMITTED_TXN_ENTRY_TYPE {
        return Err(StorageError::Codec(format!(
            "expected committed transaction entry, got type byte {:#04x}",
            raw.entry_type
        )));
    }
    crate::unified::tapir::vlog_codec::deserialize_committed_txn_payload(&raw.payload)
}

pub(crate) fn iter_committed_txn_entries<
    K: serde::de::DeserializeOwned,
    V: serde::de::DeserializeOwned,
    IO: DiskIo,
>(
    segment: &VlogSegment<IO>,
) -> Result<Vec<(u64, CachedPrepare<K, V>)>, StorageError> {
    let mut out = Vec::new();
    for (offset, raw) in segment.iter_raw_entries()? {
        if raw.entry_type != TAPIR_COMMITTED_TXN_ENTRY_TYPE {
            continue;
        }
        let ptr = VlogPtr {
            segment_id: segment.id,
            offset,
            length: RAW_ENTRY_OVERHEAD + raw.payload.len() as u32,
        };
        let prepared = read_committed_txn_from_segment(segment, &ptr)?;
        out.push((offset, prepared));
    }
    Ok(out)
}

fn recover_segment<
    K: Ord + Clone + serde::de::DeserializeOwned,
    V: serde::de::DeserializeOwned,
    IO: DiskIo,
>(
    state: &mut TapirState<K, V, IO>,
    segment: &VlogSegment<IO>,
) -> Result<(), StorageError> {
    for (offset, raw) in segment.iter_raw_entries()? {
        if raw.entry_type != TAPIR_COMMITTED_TXN_ENTRY_TYPE {
            continue;
        }

        let prepared = crate::unified::tapir::vlog_codec::deserialize_committed_txn_payload::<K, V>(&raw.payload)?;
        let txn_id = prepared.transaction_id;
        let commit_ts = prepared.commit_ts;
        let read_set = prepared.read_set;
        let write_set = prepared.write_set;

        let txn_ptr = VlogPtr {
            segment_id: segment.id,
            offset,
            length: RAW_ENTRY_OVERHEAD + raw.payload.len() as u32,
        };
        state.index_committed_txn_ptr(txn_id, txn_ptr);

        for (i, (key, value)) in write_set.iter().enumerate() {
            let value_ref = if value.is_some() {
                Some(ValueLocation::OnDisk(VlogTransactionPtr {
                    txn_ptr,
                    write_index: i as u16,
                }))
            } else {
                None
            };
            state.unified_memtable_mut().insert(
                key.clone(),
                commit_ts,
                LsmEntry {
                    value_ref,
                    last_read_ts: None,
                },
            );
        }

        for (key, read) in &read_set {
            state.unified_memtable_mut().update_last_read(key, *read, commit_ts.time);
        }
    }

    Ok(())
}

pub(crate) fn open<K: Key, V: Value, IO: DiskIo>(
    base_dir: &Path,
    io_flags: OpenFlags,
) -> Result<TapirState<K, V, IO>, StorageError> {
    IO::create_dir_all(base_dir)?;

    let manifest = UnifiedManifest::load::<IO>(base_dir)?.unwrap_or_else(UnifiedManifest::new);

    let mut sealed_vlog_segments = BTreeMap::new();
    for seg_meta in &manifest.sealed_vlog_segments {
        let seg = VlogSegment::<IO>::open_at(
            seg_meta.segment_id,
            seg_meta.path.clone(),
            seg_meta.total_size,
            seg_meta.views.clone(),
            io_flags,
        )?;
        sealed_vlog_segments.insert(seg_meta.segment_id, seg);
    }

    let active_path = base_dir.join(format!("vlog_seg_{:04}.dat", manifest.active_segment_id));
    let mut active_vlog = VlogSegment::<IO>::open_at(
        manifest.active_segment_id,
        active_path,
        manifest.active_write_offset,
        Vec::new(),
        io_flags,
    )?;
    active_vlog.start_view(manifest.current_view);

    let mut state = new_store_state::<K, V, IO>(
        DEFAULT_PREPARE_CACHE_CAPACITY,
        active_vlog,
        sealed_vlog_segments,
        manifest,
        base_dir.to_path_buf(),
        io_flags,
    );

    for seg_meta in state.manifest.sealed_vlog_segments.clone() {
        let seg = VlogSegment::<IO>::open_at(
            seg_meta.segment_id,
            seg_meta.path,
            seg_meta.total_size,
            seg_meta.views,
            state.io_flags,
        )?;
        recover_segment(&mut state, &seg)?;
        seg.close();
    }

    let active_for_recover = VlogSegment::<IO>::open_at(
        state.manifest.active_segment_id,
        state
            .base_dir
            .join(format!("vlog_seg_{:04}.dat", state.manifest.active_segment_id)),
        state.manifest.active_write_offset,
        Vec::new(),
        state.io_flags,
    )?;
    recover_segment(&mut state, &active_for_recover)?;
    active_for_recover.close();

    Ok(state)
}

impl<
        K: Key + serde::Serialize + serde::de::DeserializeOwned,
        V: Value + serde::Serialize + serde::de::DeserializeOwned,
        IO: DiskIo,
    > TapirState<K, V, IO>
{
    pub(crate) fn do_uncommitted_get(&self, key: &K) -> Result<(Option<V>, Timestamp), StorageError> {
        if let Some((ck, entry)) = self.unified_memtable().get_latest(key) {
            let ts = ck.timestamp.0;
            let value = resolve_value(self, entry)?;
            return Ok((value, ts));
        }
        Ok((None, Timestamp::default()))
    }

    pub(crate) fn do_uncommitted_get_at(&self, key: &K, ts: Timestamp) -> Result<(Option<V>, Timestamp), StorageError> {
        if let Some((ck, entry)) = self.unified_memtable().get_at(key, ts) {
            let write_ts = ck.timestamp.0;
            let value = resolve_value(self, entry)?;
            return Ok((value, write_ts));
        }
        Ok((None, Timestamp::default()))
    }

    pub(crate) fn do_uncommitted_scan(
        &self,
        start: &K,
        end: &K,
        ts: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, StorageError> {
        let mut out = Vec::new();
        for (ck, entry) in self.unified_memtable().scan(start, end, ts) {
            let value = resolve_value(self, entry)?;
            out.push((ck.key.clone(), value, ck.timestamp.0));
        }
        Ok(out)
    }

    pub(crate) fn read_committed_txn(
        &self,
        txn_ptr: &VlogPtr,
    ) -> Result<CachedPrepare<K, V>, StorageError> {
        let segment_id = txn_ptr.segment_id;
        let segment = self
            .active_or_sealed_segment(segment_id)
            .ok_or_else(|| StorageError::Codec(format!(
                "VLog segment {segment_id} not found for prepare resolution"
            )))?;

        read_committed_txn_from_segment(segment, txn_ptr)
    }
}

pub(crate) fn commit_transaction_data<
    K: Key,
    V: Value,
    IO: DiskIo,
>(
    store: &mut TapirState<K, V, IO>,
    txn_id: OccTransactionId,
    read_set: &[(K, Timestamp)],
    write_set: &[(K, Option<V>)],
    scan_set: &[(K, K, Timestamp)],
    commit: Timestamp,
) -> Result<(), StorageError> {
    let cached_prepare = Arc::new(CachedPrepare {
        transaction_id: txn_id,
        commit_ts: commit,
        read_set: read_set.to_vec(),
        write_set: write_set.to_vec(),
        scan_set: scan_set.to_vec(),
    });
    store.register_prepare_raw(txn_id, cached_prepare);

    let txn_ptr = store.append_committed_txn_to_active(txn_id, commit, read_set, write_set, scan_set)?;
    store.index_committed_txn_ptr(txn_id, txn_ptr);

    for (i, (key, value)) in write_set.iter().enumerate() {
        let value_ref = if value.is_some() {
            Some(ValueLocation::InMemory {
                txn_id,
                write_index: i as u16,
            })
        } else {
            None
        };

        store.unified_memtable_mut().insert(
            key.clone(),
            commit,
            LsmEntry {
                value_ref,
                last_read_ts: None,
            },
        );
    }

    for (key, read) in read_set {
        store
            .unified_memtable_mut()
            .update_last_read(key, *read, commit.time);
    }

    Ok(())
}

pub(crate) fn finalize_after_seal<K: Ord + Clone, V, IO: DiskIo>(state: &mut TapirState<K, V, IO>) {
    state.convert_in_memory_to_on_disk();
    state.clear_prepare_registry();
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn register_prepare<K: Key, V: Value, IO: DiskIo>(
    store: &mut TapirState<K, V, IO>,
    txn_id: OccTransactionId,
    transaction: &crate::occ::Transaction<K, V, Timestamp>,
    commit_ts: Timestamp,
) {
    let shard = crate::tapir::ShardNumber(0);

    let read_set: Vec<(K, Timestamp)> = transaction
        .shard_read_set(shard)
        .map(|(k, ts)| (k.clone(), ts))
        .collect();

    let write_set: Vec<(K, Option<V>)> = transaction
        .shard_write_set(shard)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let scan_set: Vec<(K, K, Timestamp)> = transaction
        .shard_scan_set(shard)
        .map(|entry| {
            (
                entry.start_key.clone(),
                entry.end_key.clone(),
                entry.timestamp,
            )
        })
        .collect();

    let prepare = Arc::new(CachedPrepare {
        transaction_id: txn_id,
        commit_ts,
        read_set,
        write_set,
        scan_set,
    });

    store.register_prepare_raw(txn_id, prepare);
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn unregister_prepare<K: Key, V: Value, IO: DiskIo>(
    store: &mut TapirState<K, V, IO>,
    txn_id: &OccTransactionId,
) {
    store.unregister_prepare(txn_id);
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn commit_prepared<
    K: Key,
    V: Value,
    IO: DiskIo,
>(
    store: &mut TapirState<K, V, IO>,
    txn_id: OccTransactionId,
    commit: Timestamp,
) -> Result<(), StorageError> {
    let prepare = store.lookup_prepare(&txn_id);
    let cross_view_ptr = store.lookup_committed_txn_ptr(&txn_id);

    if let Some(prepare) = prepare {
        return commit_transaction_data(
            store,
            txn_id,
            &prepare.read_set,
            &prepare.write_set,
            &prepare.scan_set,
            commit,
        );
    }

    if let Some(vlog_ptr) = cross_view_ptr {
        let cached = resolve_on_disk(store, &VlogTransactionPtr {
            txn_ptr: vlog_ptr,
            write_index: 0,
        })?;

        store.register_prepare_raw(txn_id, cached.clone());

        for (i, (key, value)) in cached.write_set.iter().enumerate() {
            let value_ref = if value.is_some() {
                Some(ValueLocation::InMemory {
                    txn_id,
                    write_index: i as u16,
                })
            } else {
                None
            };
            store.unified_memtable_mut().insert(
                key.clone(),
                commit,
                LsmEntry {
                    value_ref,
                    last_read_ts: None,
                },
            );
        }

        for (key, read) in &cached.read_set {
            store
                .unified_memtable_mut()
                .update_last_read(key, *read, commit.time);
        }
        return Ok(());
    }

    Err(StorageError::PrepareNotFound {
        client_id: txn_id.client_id.0,
        txn_number: txn_id.number,
    })
}

pub(crate) fn resolve_in_memory<K: Key, V: Value, IO: DiskIo>(
    store: &TapirState<K, V, IO>,
    txn_id: &OccTransactionId,
    write_index: u16,
) -> Option<(K, Option<V>)> {
    store.lookup_prepare_value_ref(txn_id, write_index)
}

pub(crate) fn resolve_on_disk<
    K: Key + serde::Serialize + serde::de::DeserializeOwned,
    V: Value + serde::Serialize + serde::de::DeserializeOwned,
    IO: DiskIo,
>(
    store: &TapirState<K, V, IO>,
    ptr: &VlogTransactionPtr,
) -> Result<Arc<CachedPrepare<K, V>>, StorageError> {
    let segment_id = ptr.txn_ptr.segment_id;
    let offset = ptr.txn_ptr.offset;

    if let Some(cached) = store.prepare_cache_get(segment_id, offset) {
        return Ok(cached);
    }

    store.increment_vlog_read_count();
    let prepare = store.read_committed_txn(&ptr.txn_ptr)?;
    let cached = Arc::new(prepare);
    store.prepare_cache_insert(segment_id, offset, cached.clone());
    Ok(cached)
}

pub(crate) fn resolve_value<
    K: Key,
    V: Value,
    IO: DiskIo,
>(
    store: &TapirState<K, V, IO>,
    entry: &LsmEntry,
) -> Result<Option<V>, StorageError> {
    match &entry.value_ref {
        None => Ok(None),
        Some(ValueLocation::InMemory { txn_id, write_index }) => {
            match resolve_in_memory(store, txn_id, *write_index) {
                Some((_key, value)) => Ok(value.clone()),
                None => Ok(None),
            }
        }
        Some(ValueLocation::OnDisk(ptr)) => {
            let cached = resolve_on_disk(store, ptr)?;
            if let Some((_key, value)) = cached.write_set.get(ptr.write_index as usize) {
                Ok(value.clone())
            } else {
                Ok(None)
            }
        }
    }
}

pub(crate) fn do_uncommitted_get<
    K: Key + serde::Serialize + serde::de::DeserializeOwned,
    V: Value + serde::Serialize + serde::de::DeserializeOwned,
    IO: DiskIo,
>(
    store: &TapirState<K, V, IO>,
    key: &K,
) -> Result<(Option<V>, Timestamp), StorageError> {
    store.do_uncommitted_get(key)
}

pub(crate) fn do_uncommitted_get_at<
    K: Key + serde::Serialize + serde::de::DeserializeOwned,
    V: Value + serde::Serialize + serde::de::DeserializeOwned,
    IO: DiskIo,
>(
    store: &TapirState<K, V, IO>,
    key: &K,
    ts: Timestamp,
) -> Result<(Option<V>, Timestamp), StorageError> {
    store.do_uncommitted_get_at(key, ts)
}

pub(crate) fn do_uncommitted_scan<
    K: Key + serde::Serialize + serde::de::DeserializeOwned,
    V: Value + serde::Serialize + serde::de::DeserializeOwned,
    IO: DiskIo,
>(
    store: &TapirState<K, V, IO>,
    start: &K,
    end: &K,
    ts: Timestamp,
) -> Result<Vec<(K, Option<V>, Timestamp)>, StorageError> {
    store.do_uncommitted_scan(start, end, ts)
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn txn_log_get<K: Key, V: Value, IO: DiskIo>(
    store: &TapirState<K, V, IO>,
    id: &TransactionId,
) -> Option<(Timestamp, bool)> {
    store.txn_log_get(id)
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn txn_log_insert<K: Key, V: Value, IO: DiskIo>(
    store: &mut TapirState<K, V, IO>,
    id: TransactionId,
    ts: Timestamp,
    committed: bool,
) -> Option<(Timestamp, bool)> {
    store.txn_log_insert(id, ts, committed)
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn txn_log_contains<K: Key, V: Value, IO: DiskIo>(
    store: &TapirState<K, V, IO>,
    id: &TransactionId,
) -> bool {
    store.txn_log_contains(id)
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn txn_log_len<K: Key, V: Value, IO: DiskIo>(store: &TapirState<K, V, IO>) -> usize {
    store.txn_log_len()
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn raise_min_prepare_time<K: Key, V: Value, IO: DiskIo>(
    store: &mut TapirState<K, V, IO>,
    time: u64,
) -> u64 {
    store.raise_min_prepare_time(time)
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn finalize_min_prepare_time<K: Key, V: Value, IO: DiskIo>(
    store: &mut TapirState<K, V, IO>,
    time: u64,
) {
    store.finalize_min_prepare_time(time);
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn sync_min_prepare_time<K: Key, V: Value, IO: DiskIo>(
    store: &mut TapirState<K, V, IO>,
    time: u64,
) {
    store.sync_min_prepare_time(time);
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn reset_min_prepare_time_to_finalized<K: Key, V: Value, IO: DiskIo>(
    store: &mut TapirState<K, V, IO>,
) {
    store.reset_min_prepare_time_to_finalized();
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn record_cdc_delta<K: Key, V: Value, IO: DiskIo>(
    store: &mut TapirState<K, V, IO>,
    base_view: u64,
    delta: LeaderRecordDelta<K, V>,
) {
    store.record_cdc_delta(base_view, delta);
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn cdc_deltas_from<K: Key, V: Value, IO: DiskIo>(
    store: &TapirState<K, V, IO>,
    from_view: u64,
) -> Vec<LeaderRecordDelta<K, V>> {
    store.cdc_deltas_from(from_view)
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn cdc_max_view<K: Key, V: Value, IO: DiskIo>(store: &TapirState<K, V, IO>) -> Option<u64> {
    store.cdc_max_view()
}

#[cfg(test)]
pub(crate) struct TapirStateRunner {
    tapir: TapirState<String, String, crate::mvcc::disk::memory_io::MemoryIo>,
    base_dir: PathBuf,
}

#[cfg(test)]
impl TapirStateRunner {
    pub(crate) fn open(base_dir: PathBuf, io_flags: OpenFlags) -> Result<Self, StorageError> {
        crate::mvcc::disk::memory_io::MemoryIo::create_dir_all(&base_dir)?;
        let tapir = open::<String, String, crate::mvcc::disk::memory_io::MemoryIo>(&base_dir, io_flags)?;
        Ok(Self {
            tapir,
            base_dir,
        })
    }

    pub(crate) fn list_dir_files(&self) -> Vec<(String, usize)> {
        let files = crate::mvcc::disk::memory_io::MemoryIo::list_files(&self.base_dir);
        let prefix = format!("{}/", self.base_dir.display());
        files
            .into_iter()
            .map(|(p, size)| {
                let path_string = p.to_string_lossy().to_string();
                let name = path_string
                    .strip_prefix(&prefix)
                    .unwrap_or(&path_string)
                    .to_string();
                (name, size)
            })
            .collect()
    }

    pub(crate) fn register_prepare(
        &mut self,
        txn_id: OccTransactionId,
        transaction: &crate::occ::Transaction<String, String, Timestamp>,
        commit_ts: Timestamp,
    ) {
        register_prepare(&mut self.tapir, txn_id, transaction, commit_ts);
    }

    pub(crate) fn commit_prepared(
        &mut self,
        txn_id: OccTransactionId,
        commit: Timestamp,
    ) -> Result<(), StorageError> {
        commit_prepared(&mut self.tapir, txn_id, commit)
    }

    pub(crate) fn seal(&mut self, min_view_vlog_size: u64) -> Result<(), StorageError> {
        self.tapir.seal_current_view(min_view_vlog_size)
    }

    pub(crate) fn get_at(
        &self,
        key: &str,
        ts: Timestamp,
    ) -> Result<(Option<String>, Timestamp), StorageError> {
        self.tapir.do_uncommitted_get_at(&key.to_string(), ts)
    }

    pub(crate) fn scan(
        &self,
        start: &str,
        end: &str,
        ts: Timestamp,
    ) -> Result<Vec<(String, Option<String>, Timestamp)>, StorageError> {
        self.tapir
            .do_uncommitted_scan(&start.to_string(), &end.to_string(), ts)
    }

    pub(crate) fn has_prepare_write_conflict(&self, key: &str) -> bool {
        self.tapir.has_prepare_write_conflict(&key.to_string())
    }

    pub(crate) fn contains_prepare(&self, txn_id: &OccTransactionId) -> bool {
        self.tapir.contains_prepare(txn_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IrClientId;

    #[test]
    fn resolve_on_disk_uses_cache_after_first_read() {
        let base_dir = std::path::PathBuf::from(format!(
            "/tapir-store-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time should be after epoch")
                .as_nanos()
        ));
        let mut runner = TapirStateRunner::open(base_dir, OpenFlags::default())
            .expect("runner should open");

        let txn_id = OccTransactionId {
            client_id: IrClientId(7),
            number: 11,
        };
        let ts = Timestamp {
            time: 42,
            client_id: IrClientId(7),
        };

        commit_transaction_data(
            &mut runner.tapir,
            txn_id,
            &[("r".to_string(), ts)],
            &[("k".to_string(), Some("v".to_string()))],
            &[],
            ts,
        )
        .expect("commit should append committed txn");
        runner.seal(0).expect("seal should finalize current view");

        let txn_ptr = runner
            .tapir
            .lookup_committed_txn_ptr(&txn_id)
            .expect("committed txn ptr should exist after seal");
        let ptr = VlogTransactionPtr {
            txn_ptr,
            write_index: 0,
        };

        let first = resolve_on_disk(&runner.tapir, &ptr).expect("first resolve should read from disk");
        assert_eq!(runner.tapir.vlog_read_count(), 1);
        assert_eq!(first.write_set[0].1.as_deref(), Some("v"));

        let second = resolve_on_disk(&runner.tapir, &ptr).expect("second resolve should hit cache");
        assert_eq!(runner.tapir.vlog_read_count(), 1);
        assert!(Arc::ptr_eq(&first, &second));
    }
}
