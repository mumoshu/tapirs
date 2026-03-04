use crate::mvcc::disk::error::StorageError;
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::{Key, Timestamp, Value};
#[cfg(test)]
use crate::tapirstore::TransactionLog;
use crate::unified::tapir::Transaction;
use crate::unified::tapir::{LsmEntry, ValueLocation, VlogSegment, VlogTransactionPtr};
#[cfg(test)]
use crate::IrClientId;
use crate::unified::tapir::memtable::Memtable;
use crate::unified::wisckeylsm::manifest::UnifiedManifest;
use crate::unified::tapir::prepare_cache::PreparedTransactions;
use crate::unified::wisckeylsm::types::{VlogPtr, VlogSegmentMeta};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) struct TapirState<K: Ord, V, IO: DiskIo> {
    memtable: Memtable<K>,
    prepared_txns_in_mem: BTreeMap<OccTransactionId, Arc<Transaction<K, V>>>,
    prepare_cache: RefCell<PreparedTransactions<Transaction<K, V>>>,
    committed_txn_vlog_index: BTreeMap<OccTransactionId, VlogPtr>,
    vlog_read_count: Cell<u64>,
    #[cfg(test)]
    txlog: TransactionLog,
    active_vlog: VlogSegment<IO>,
    sealed_vlog_segments: BTreeMap<u64, VlogSegment<IO>>,
    manifest: UnifiedManifest,
    base_dir: PathBuf,
    io_flags: OpenFlags,
    current_view_entry_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatusCommit<K, V> {
    pub(crate) offset: u64,
    pub(crate) transaction_id: OccTransactionId,
    pub(crate) commit_ts: Timestamp,
    pub(crate) write_set: Vec<(K, Option<V>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatusSegment<K, V> {
    pub(crate) id: u64,
    pub(crate) size: u64,
    pub(crate) views: Vec<u64>,
    pub(crate) commits: Vec<StatusCommit<K, V>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatusReport<K, V> {
    pub(crate) view: u64,
    pub(crate) sealed_segments: usize,
    pub(crate) segments: Vec<StatusSegment<K, V>>,
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
        memtable: Memtable::new(),
        prepared_txns_in_mem: BTreeMap::new(),
        prepare_cache: RefCell::new(PreparedTransactions::new(prepare_cache_capacity)),
        committed_txn_vlog_index: BTreeMap::new(),
        vlog_read_count: Cell::new(0),
        #[cfg(test)]
        txlog: TransactionLog::new(),
        active_vlog,
        sealed_vlog_segments,
        manifest,
        base_dir,
        io_flags,
        current_view_entry_count: 0,
    }
}

impl<K: Ord + Clone, V, IO: DiskIo> TapirState<K, V, IO> {
    fn vlog_read_count(&self) -> u64 {
        self.vlog_read_count.get()
    }

    fn increment_vlog_read_count(&self) {
        self.vlog_read_count.set(self.vlog_read_count.get() + 1);
    }

    fn insert_memtable_entry(
        &mut self,
        key: K,
        ts: Timestamp,
        entry: LsmEntry,
    ) {
        self.memtable.insert(key, ts, entry);
    }

    fn update_memtable_last_read(&mut self, key: &K, read: Timestamp, commit_time: u64) {
        self.memtable.update_last_read(key, read, commit_time);
    }

    #[cfg(test)]
    fn memtable_entry_at(&self, key: &K, ts: Timestamp) -> Option<&LsmEntry> {
        self.memtable.get_at(key, ts).map(|(_, entry)| entry)
    }

    pub(crate) fn abort(&mut self, txn_id: &OccTransactionId) {
        self.prepared_txns_in_mem.remove(txn_id);
    }

    #[cfg(test)]
    fn lookup_prepare(&self, txn_id: &OccTransactionId) -> Option<Arc<Transaction<K, V>>> {
        self.prepared_txns_in_mem.get(txn_id).cloned()
    }

    fn lookup_prepare_value_ref(
        &self,
        txn_id: &OccTransactionId,
        write_index: u16,
    ) -> Option<(K, Option<V>)>
    where
        K: Clone,
        V: Clone,
    {
        self.prepared_txns_in_mem
            .get(txn_id)
            .and_then(|p| p.write_set.get(write_index as usize))
            .cloned()
    }

    #[cfg(test)]
    fn resolve_in_memory_ref(
        &self,
        txn_id: &OccTransactionId,
        write_index: u16,
    ) -> Option<&(K, Option<V>)> {
        self.prepared_txns_in_mem
            .get(txn_id)
            .and_then(|p| p.write_set.get(write_index as usize))
    }

    #[cfg(test)]
    fn has_prepare_write_conflict(&self, key: &K) -> bool {
        self.prepared_txns_in_mem
            .values()
            .flat_map(|p| p.write_set.iter())
            .any(|(k, _)| k == key)
    }

    #[cfg(test)]
    fn contains_prepare(&self, txn_id: &OccTransactionId) -> bool {
        self.prepared_txns_in_mem.contains_key(txn_id)
    }

    fn clear_prepare_registry(&mut self) {
        self.prepared_txns_in_mem.clear();
    }

    fn index_committed_txn_ptr(&mut self, txn_id: OccTransactionId, ptr: VlogPtr) {
        self.committed_txn_vlog_index.insert(txn_id, ptr);
    }

    #[cfg(test)]
    fn lookup_committed_txn_ptr(&self, txn_id: &OccTransactionId) -> Option<VlogPtr> {
        self.committed_txn_vlog_index.get(txn_id).copied()
    }

    fn prepare_cache_get(
        &self,
        segment_id: u64,
        offset: u64,
    ) -> Option<Arc<Transaction<K, V>>> {
        self.prepare_cache.borrow_mut().get(segment_id, offset)
    }

    fn prepare_cache_insert(
        &self,
        segment_id: u64,
        offset: u64,
        prepare: Arc<Transaction<K, V>>,
    ) {
        self.prepare_cache
            .borrow_mut()
            .insert(segment_id, offset, prepare);
    }

    #[cfg(test)]
    fn txn_log_get(&self, id: &OccTransactionId) -> Option<(Timestamp, bool)> {
        self.txlog.txn_log_get(id)
    }

    #[cfg(test)]
    fn txn_log_insert(
        &mut self,
        id: OccTransactionId,
        ts: Timestamp,
        committed: bool,
    ) -> Option<(Timestamp, bool)> {
        self.txlog.txn_log_insert(id, ts, committed)
    }

    #[cfg(test)]
    fn txn_log_contains(&self, id: &OccTransactionId) -> bool {
        self.txlog.txn_log_contains(id)
    }

    #[cfg(test)]
    fn txn_log_len(&self) -> usize {
        self.txlog.txn_log_len()
    }

    #[cfg(test)]
    fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    fn current_view(&self) -> u64 {
        self.manifest.current_view
    }

    fn sealed_vlog_segments(&self) -> &BTreeMap<u64, VlogSegment<IO>> {
        &self.sealed_vlog_segments
    }

    fn active_vlog_id(&self) -> u64 {
        self.active_vlog.id
    }

    fn active_or_sealed_segment_ref(&self, segment_id: u64) -> Option<&VlogSegment<IO>> {
        self.sealed_vlog_segments
            .get(&segment_id)
            .or(if self.active_vlog.id == segment_id {
                Some(&self.active_vlog)
            } else {
                None
            })
    }

    fn get_range(
        &self,
        key: &K,
        timestamp: Timestamp,
    ) -> Result<(Timestamp, Option<Timestamp>), StorageError> {
        if let Some((ck, _entry)) = self.memtable.get_at(key, timestamp) {
            let write_ts = ck.timestamp.0;
            let next = self.memtable.find_next_version(key, write_ts);
            return Ok((write_ts, next));
        }
        Ok((Timestamp::default(), None))
    }

    fn has_writes_in_range(
        &self,
        start: &K,
        end: &K,
        after_ts: Timestamp,
        before_ts: Timestamp,
    ) -> Result<bool, StorageError> {
        Ok(self
            .memtable
            .has_writes_in_range(start, end, after_ts, before_ts))
    }

    #[cfg(test)]
    fn commit_get(
        &mut self,
        key: K,
        read: Timestamp,
        commit: Timestamp,
    ) -> Result<(), StorageError> {
        self.update_memtable_last_read(&key, read, commit.time);
        Ok(())
    }

    #[cfg(test)]
    fn get_last_read(&self, key: &K) -> Result<Option<Timestamp>, StorageError> {
        if let Some((_, entry)) = self.memtable.get_latest(key)
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
    fn get_last_read_at(
        &self,
        key: &K,
        timestamp: Timestamp,
    ) -> Result<Option<Timestamp>, StorageError> {
        if let Some((_, entry)) = self.memtable.get_at(key, timestamp)
            && let Some(ts) = entry.last_read_ts
        {
            return Ok(Some(Timestamp {
                time: ts,
                client_id: IrClientId(1),
            }));
        }
        Ok(None)
    }

    pub(crate) fn commit(
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
        self.prepared_txns_in_mem.insert(
            txn_id,
            Arc::new(Transaction {
                transaction_id: txn_id,
                commit_ts: commit,
                read_set: read_set.to_vec(),
                write_set: write_set.to_vec(),
                scan_set: scan_set.to_vec(),
            }),
        );

        let txn_ptr =
            self.append_committed_txn_to_active(txn_id, commit, read_set, write_set, scan_set)?;
        self.index_committed_txn_ptr(txn_id, txn_ptr);

        for (i, (key, value)) in write_set.iter().enumerate() {
            let value_ref = if value.is_some() {
                Some(ValueLocation::InMemory {
                    txn_id,
                    write_index: i as u16,
                })
            } else {
                None
            };

            self.insert_memtable_entry(
                key.clone(),
                commit,
                LsmEntry {
                    value_ref,
                    last_read_ts: None,
                },
            );
        }

        for (key, read) in read_set {
            self.update_memtable_last_read(key, *read, commit.time);
        }

        Ok(())
    }

    pub(crate) fn commit_prepared(
        &mut self,
        txn_id: OccTransactionId,
        commit: Timestamp,
    ) -> Result<(), StorageError>
    where
        K: Key,
        V: Value,
    {
        let prepared = self
            .prepared_txns_in_mem
            .get(&txn_id)
            .cloned()
            .ok_or_else(|| StorageError::Codec(format!(
                "no prepared transaction for commit: {}:{}",
                txn_id.client_id.0, txn_id.number
            )))?;

        self.commit(
            txn_id,
            &prepared.read_set,
            &prepared.write_set,
            &prepared.scan_set,
            commit,
        )
    }

    pub(crate) fn prepare(
        &mut self,
        txn_id: OccTransactionId,
        transaction: &crate::occ::Transaction<K, V, Timestamp>,
        commit_ts: Timestamp,
    )
    where
        K: Key,
        V: Value,
    {
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

        let prepare = Arc::new(Transaction {
            transaction_id: txn_id,
            commit_ts,
            read_set,
            write_set,
            scan_set,
        });

        self.prepared_txns_in_mem.insert(txn_id, prepare);
    }

    fn convert_in_memory_to_on_disk(&mut self) {
        let committed_index = self.committed_txn_vlog_index.clone();
        self.memtable
            .convert_in_memory_to_on_disk(&committed_index);
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
        let payload = crate::unified::tapir::vlog_codec::serialize_committed_txn_payload(
            txn_id,
            commit,
            read_set,
            write_set,
            scan_set,
        )?;
        let ptr = self.active_vlog.append_raw_entry(
            TAPIR_COMMITTED_TXN_ENTRY_TYPE,
            txn_id.client_id.0,
            txn_id.number,
            &payload,
        )?;
        self.current_view_entry_count += 1;
        Ok(ptr)
    }

    fn recover_segment(&mut self, segment: &VlogSegment<IO>) -> Result<(), StorageError>
    where
        K: serde::de::DeserializeOwned,
        V: serde::de::DeserializeOwned,
    {
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
            self.index_committed_txn_ptr(txn_id, txn_ptr);

            for (i, (key, value)) in write_set.iter().enumerate() {
                let value_ref = if value.is_some() {
                    Some(ValueLocation::OnDisk(VlogTransactionPtr {
                        txn_ptr,
                        write_index: i as u16,
                    }))
                } else {
                    None
                };
                self.insert_memtable_entry(
                    key.clone(),
                    commit_ts,
                    LsmEntry {
                        value_ref,
                        last_read_ts: None,
                    },
                );
            }

            for (key, read) in &read_set {
                self.update_memtable_last_read(key, *read, commit_ts.time);
            }
        }

        Ok(())
    }

    #[cfg(test)]
    fn resolve_in_memory(
        &self,
        txn_id: &OccTransactionId,
        write_index: u16,
    ) -> Option<&(K, Option<V>)> {
        self.resolve_in_memory_ref(txn_id, write_index)
    }

    #[cfg(test)]
    fn resolve_value(
        &self,
        entry: &LsmEntry,
    ) -> Result<Option<V>, StorageError>
    where
        K: Key + serde::Serialize + serde::de::DeserializeOwned,
        V: Value + serde::Serialize + serde::de::DeserializeOwned,
    {
        self.resolve_value_internal(entry)
    }

    fn resolve_in_memory_owned(
        &self,
        txn_id: &OccTransactionId,
        write_index: u16,
    ) -> Option<(K, Option<V>)>
    where
        K: Clone,
        V: Clone,
    {
        self.lookup_prepare_value_ref(txn_id, write_index)
    }

    fn resolve_on_disk(
        &self,
        ptr: &VlogTransactionPtr,
    ) -> Result<Arc<Transaction<K, V>>, StorageError>
    where
        K: Key + serde::Serialize + serde::de::DeserializeOwned,
        V: Value + serde::Serialize + serde::de::DeserializeOwned,
    {
        let segment_id = ptr.txn_ptr.segment_id;
        let offset = ptr.txn_ptr.offset;

        if let Some(cached) = self.prepare_cache_get(segment_id, offset) {
            return Ok(cached);
        }

        self.increment_vlog_read_count();
        let prepare = self.read_committed_txn(&ptr.txn_ptr)?;
        let cached = Arc::new(prepare);
        self.prepare_cache_insert(segment_id, offset, cached.clone());
        Ok(cached)
    }

    fn resolve_value_internal(&self, entry: &LsmEntry) -> Result<Option<V>, StorageError>
    where
        K: Key + serde::Serialize + serde::de::DeserializeOwned,
        V: Value + serde::Serialize + serde::de::DeserializeOwned,
    {
        match &entry.value_ref {
            None => Ok(None),
            Some(ValueLocation::InMemory { txn_id, write_index }) => {
                match self.resolve_in_memory_owned(txn_id, *write_index) {
                    Some((_key, value)) => Ok(value.clone()),
                    None => Ok(None),
                }
            }
            Some(ValueLocation::OnDisk(ptr)) => {
                let cached = self.resolve_on_disk(ptr)?;
                if let Some((_key, value)) = cached.write_set.get(ptr.write_index as usize) {
                    Ok(value.clone())
                } else {
                    Ok(None)
                }
            }
        }
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

        self.convert_in_memory_to_on_disk();
        self.clear_prepare_registry();
        self.current_view_entry_count = 0;
        Ok(())
    }

    pub(crate) fn status(&self) -> Result<StatusReport<K, V>, StorageError>
    where
        K: serde::de::DeserializeOwned + Clone,
        V: serde::de::DeserializeOwned + Clone,
    {
        let mut segments = Vec::new();

        let mut append_segment = |seg: &VlogSegment<IO>| -> Result<(), StorageError> {
            let commits = iter_committed_txn_entries::<K, V, _>(seg)?
                .into_iter()
                .map(|(offset, prepared)| StatusCommit {
                    offset,
                    transaction_id: prepared.transaction_id,
                    commit_ts: prepared.commit_ts,
                    write_set: prepared.write_set,
                })
                .collect();

            segments.push(StatusSegment {
                id: seg.id,
                size: seg.write_offset(),
                views: seg.views.iter().map(|view| view.view).collect(),
                commits,
            });
            Ok(())
        };

        append_segment(&self.active_vlog)?;
        for seg in self.sealed_vlog_segments.values() {
            append_segment(seg)?;
        }

        Ok(StatusReport {
            view: self.current_view(),
            sealed_segments: self.sealed_vlog_segments().len(),
            segments,
        })
    }
}

const DEFAULT_PREPARE_CACHE_CAPACITY: usize = 1024;
const RAW_ENTRY_OVERHEAD: u32 = 25;
const TAPIR_COMMITTED_TXN_ENTRY_TYPE: u8 = 0x80;

fn read_committed_txn_from_segment<
    K: serde::de::DeserializeOwned,
    V: serde::de::DeserializeOwned,
    IO: DiskIo,
>(
    segment: &VlogSegment<IO>,
    txn_ptr: &VlogPtr,
) -> Result<Transaction<K, V>, StorageError> {
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
) -> Result<Vec<(u64, Transaction<K, V>)>, StorageError> {
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
        state.recover_segment(&seg)?;
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
    state.recover_segment(&active_for_recover)?;
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
        if let Some((ck, entry)) = self.memtable.get_latest(key) {
            let ts = ck.timestamp.0;
            let value = self.resolve_value_internal(entry)?;
            return Ok((value, ts));
        }
        Ok((None, Timestamp::default()))
    }

    pub(crate) fn do_uncommitted_get_at(&self, key: &K, ts: Timestamp) -> Result<(Option<V>, Timestamp), StorageError> {
        if let Some((ck, entry)) = self.memtable.get_at(key, ts) {
            let write_ts = ck.timestamp.0;
            let value = self.resolve_value_internal(entry)?;
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
        for (ck, entry) in self.memtable.scan(start, end, ts) {
            let value = self.resolve_value_internal(entry)?;
            out.push((ck.key.clone(), value, ck.timestamp.0));
        }
        Ok(out)
    }

    fn read_committed_txn(
        &self,
        txn_ptr: &VlogPtr,
    ) -> Result<Transaction<K, V>, StorageError> {
        let segment_id = txn_ptr.segment_id;
        let segment = self
            .active_or_sealed_segment_ref(segment_id)
            .ok_or_else(|| StorageError::Codec(format!(
                "VLog segment {segment_id} not found for prepare resolution"
            )))?;

        read_committed_txn_from_segment(segment, txn_ptr)
    }
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
        self.tapir.prepare(txn_id, transaction, commit_ts);
    }

    pub(crate) fn commit_transaction_data(
        &mut self,
        txn_id: OccTransactionId,
        read_set: &[(String, Timestamp)],
        write_set: &[(String, Option<String>)],
        scan_set: &[(String, String, Timestamp)],
        commit_ts: Timestamp,
    ) -> Result<(), StorageError> {
        self.tapir
            .commit(txn_id, read_set, write_set, scan_set, commit_ts)
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

        runner
            .tapir
            .commit(
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

        let first = runner.tapir.resolve_on_disk(&ptr).expect("first resolve should read from disk");
        assert_eq!(runner.tapir.vlog_read_count(), 1);
        assert_eq!(first.write_set[0].1.as_deref(), Some("v"));

        let second = runner.tapir.resolve_on_disk(&ptr).expect("second resolve should hit cache");
        assert_eq!(runner.tapir.vlog_read_count(), 1);
        assert!(Arc::ptr_eq(&first, &second));
    }
}
