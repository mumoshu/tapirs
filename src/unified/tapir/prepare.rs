use crate::mvcc::disk::error::StorageError;
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::IrClientId;
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::{Key, Timestamp, Value};
use crate::unified::tapir::Transaction;
use crate::unified::tapir::{LsmEntry, ValueLocation, VlogSegment, VlogTransactionPtr};
use crate::unified::tapir::memtable::Memtable;
use crate::unified::wisckeylsm::lsm::VlogLsm;
use crate::unified::wisckeylsm::manifest::UnifiedManifest;
use crate::unified::tapir::prepare_cache::PreparedTransactions;
use crate::unified::wisckeylsm::types::VlogPtr;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) struct TapirState<K: Ord, V, IO: DiskIo> {
    /// MVCC index: maps (key, timestamp) → ValueLocation for get/get_at/scan.
    /// Rebuilt on recovery by scanning committed vlog segments (see `recover_segment`),
    /// which inserts OnDisk entries pointing into the committed VlogLsm.
    memtable: Memtable<K>,
    /// Committed transaction store: txn_id → serialized Transaction in vlog.
    /// On commit, the transaction is written here and MVCC entries point into it.
    committed: VlogLsm<OccTransactionId, Arc<Transaction<K, V>>, IO>,
    /// Prepared transaction store: txn_id → serialized Transaction in vlog.
    /// Provides crash durability for in-flight prepares. Cleared on commit/abort.
    prepared: VlogLsm<OccTransactionId, Arc<Transaction<K, V>>, IO>,
    /// LRU cache for deserialized transactions read from sealed vlog segments.
    prepare_cache: RefCell<PreparedTransactions<Transaction<K, V>>>,
    /// Number of vlog reads performed (for cache hit rate tracking in tests).
    vlog_read_count: Cell<u64>,
    /// Persisted metadata: view number, vlog segment positions, SST lists.
    manifest: UnifiedManifest,
    /// Root directory for all store files (vlogs, SSTs, manifest).
    base_dir: PathBuf,
    /// File open flags (create, direct I/O).
    io_flags: OpenFlags,
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

const DEFAULT_PREPARE_CACHE_CAPACITY: usize = 1024;
const RAW_ENTRY_OVERHEAD: u32 = 25;
const TAPIR_COMMITTED_TXN_ENTRY_TYPE: u8 = 0x80;
const TAPIR_PREPARED_TXN_ENTRY_TYPE: u8 = 0x81;

impl<K: Ord + Clone, V, IO: DiskIo> TapirState<K, V, IO> {
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

    pub(crate) fn abort(&mut self, txn_id: &OccTransactionId) {
        self.prepared.remove(txn_id);
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

    fn current_view(&self) -> u64 {
        self.manifest.current_view
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
        self.prepared.remove(&txn_id);

        let arc_txn = Arc::new(Transaction {
            transaction_id: txn_id,
            commit_ts: commit,
            read_set: read_set.to_vec(),
            write_set: write_set.to_vec(),
            scan_set: scan_set.to_vec(),
        });

        self.committed.put(txn_id, arc_txn);

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

    fn does_prepare_conflict(
        &self,
        write_set: &[(K, Option<V>)],
    ) -> Result<bool, StorageError>
    where
        K: Key + serde::Serialize + serde::de::DeserializeOwned,
        V: Value + serde::Serialize + serde::de::DeserializeOwned,
    {
        if write_set.is_empty() {
            return Ok(false);
        }

        let new_keys: std::collections::BTreeSet<&K> =
            write_set.iter().map(|(k, _)| k).collect();

        // Check memtable entries (current view's prepared txns)
        for txn in self.prepared.memtable_values() {
            for (key, _) in &txn.write_set {
                if new_keys.contains(key) {
                    return Ok(true);
                }
            }
        }

        // Check index entries (sealed views' prepared txns, loaded from vlog)
        for txn_id in self.prepared.index().keys() {
            if let Some(txn) = self.prepared.get(txn_id)? {
                for (key, _) in &txn.write_set {
                    if new_keys.contains(key) {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    pub(crate) fn prepare(
        &mut self,
        txn_id: OccTransactionId,
        transaction: &crate::occ::Transaction<K, V, Timestamp>,
        commit_ts: Timestamp,
    ) -> Result<(), StorageError>
    where
        K: Key + serde::Serialize + serde::de::DeserializeOwned,
        V: Value + serde::Serialize + serde::de::DeserializeOwned,
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

        if self.does_prepare_conflict(&write_set)? {
            return Err(StorageError::Codec(
                format!("prepare conflict: transaction {txn_id:?} writes to keys already prepared"),
            ));
        }

        let prepare = Arc::new(Transaction {
            transaction_id: txn_id,
            commit_ts,
            read_set,
            write_set,
            scan_set: Vec::new(),
        });

        self.prepared.put(txn_id, prepare);
        Ok(())
    }

    fn convert_in_memory_to_on_disk(&mut self) {
        let committed_index = self.committed.index().clone();
        self.memtable
            .convert_in_memory_to_on_disk(&committed_index);
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

            let txn: Transaction<K, V> = bitcode::deserialize(&raw.payload)
                .map_err(|e| StorageError::Codec(e.to_string()))?;
            let txn_id = txn.transaction_id;
            let commit_ts = txn.commit_ts;

            let txn_ptr = VlogPtr {
                segment_id: segment.id,
                offset,
                length: RAW_ENTRY_OVERHEAD + raw.payload.len() as u32,
            };
            self.committed.index_insert(txn_id, txn_ptr);

            for (i, (key, value)) in txn.write_set.iter().enumerate() {
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

            for (key, read) in &txn.read_set {
                self.update_memtable_last_read(key, *read, commit_ts.time);
            }
        }

        Ok(())
    }

    fn recover_prepared_segment(
        &mut self,
        segment: &VlogSegment<IO>,
    ) -> Result<(), StorageError> {
        for (offset, raw) in segment.iter_raw_entries()? {
            if raw.entry_type != TAPIR_PREPARED_TXN_ENTRY_TYPE {
                continue;
            }

            let txn_id = OccTransactionId {
                client_id: IrClientId(raw.id_client),
                number: raw.id_number,
            };
            let txn_ptr = VlogPtr {
                segment_id: segment.id,
                offset,
                length: RAW_ENTRY_OVERHEAD + raw.payload.len() as u32,
            };
            self.prepared.index_insert(txn_id, txn_ptr);
        }

        Ok(())
    }

    fn resolve_value_internal(&self, entry: &LsmEntry) -> Result<Option<V>, StorageError>
    where
        K: Key + serde::Serialize + serde::de::DeserializeOwned,
        V: Value + serde::Serialize + serde::de::DeserializeOwned,
    {
        match &entry.value_ref {
            None => Ok(None),
            Some(ValueLocation::InMemory { txn_id, write_index }) => {
                match self.committed.get(txn_id)? {
                    Some(txn) => {
                        Ok(txn.write_set.get(*write_index as usize).and_then(|(_, v)| v.clone()))
                    }
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

    pub(crate) fn seal_current_view(&mut self, min_view_vlog_size: u64) -> Result<(), StorageError>
    where
        K: serde::Serialize,
        V: serde::Serialize,
    {
        // FIRST: flush memtable→vlog+index, then clear memtable
        let sealed_meta =
            self.committed
                .seal_view(min_view_vlog_size, |txn_id, _txn| {
                    Some((
                        TAPIR_COMMITTED_TXN_ENTRY_TYPE,
                        txn_id.client_id.0,
                        txn_id.number,
                    ))
                })?;
        if let Some(meta) = sealed_meta {
            self.manifest.committed.sealed_vlog_segments.push(meta);
        }

        let prepared_sealed =
            self.prepared
                .seal_view(min_view_vlog_size, |txn_id, _txn| {
                    Some((
                        TAPIR_PREPARED_TXN_ENTRY_TYPE,
                        txn_id.client_id.0,
                        txn_id.number,
                    ))
                })?;
        if let Some(meta) = prepared_sealed {
            self.manifest.prepared.sealed_vlog_segments.push(meta);
        }

        // THEN: convert InMemory→OnDisk (index now populated by seal above)
        self.convert_in_memory_to_on_disk();

        self.manifest.current_view += 1;
        self.manifest.committed.active_segment_id = self.committed.active_vlog_id();
        self.manifest.committed.active_write_offset = self.committed.active_write_offset();
        self.manifest.committed.next_segment_id = self.committed.next_segment_id();
        self.manifest.prepared.active_segment_id = self.prepared.active_vlog_id();
        self.manifest.prepared.active_write_offset = self.prepared.active_write_offset();
        self.manifest.prepared.next_segment_id = self.prepared.next_segment_id();
        self.manifest.save::<IO>(&self.base_dir)?;
        self.committed.start_view(self.manifest.current_view);
        self.prepared.start_view(self.manifest.current_view);
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
                .map(|(offset, txn)| StatusCommit {
                    offset,
                    transaction_id: txn.transaction_id,
                    commit_ts: txn.commit_ts,
                    write_set: txn.write_set,
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

        append_segment(self.committed.active_vlog_ref())?;
        for seg in self.committed.sealed_segments_ref().values() {
            append_segment(seg)?;
        }

        Ok(StatusReport {
            view: self.current_view(),
            sealed_segments: self.committed.sealed_segments_ref().len(),
            segments,
        })
    }
}

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
    bitcode::deserialize(&raw.payload).map_err(|e| StorageError::Codec(e.to_string()))
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
        let txn = read_committed_txn_from_segment(segment, &ptr)?;
        out.push((offset, txn));
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
    for seg_meta in &manifest.committed.sealed_vlog_segments {
        let seg = VlogSegment::<IO>::open_at(
            seg_meta.segment_id,
            seg_meta.path.clone(),
            seg_meta.total_size,
            seg_meta.views.clone(),
            io_flags,
        )?;
        sealed_vlog_segments.insert(seg_meta.segment_id, seg);
    }

    let active_path = base_dir.join(format!("vlog_seg_{:04}.dat", manifest.committed.active_segment_id));
    let mut active_vlog = VlogSegment::<IO>::open_at(
        manifest.committed.active_segment_id,
        active_path,
        manifest.committed.active_write_offset,
        Vec::new(),
        io_flags,
    )?;
    active_vlog.start_view(manifest.current_view);

    let committed = VlogLsm::open_from_parts(
        "",
        base_dir,
        active_vlog,
        sealed_vlog_segments,
        io_flags,
        manifest.committed.next_segment_id,
        usize::MAX,
    );

    let mut prepared_sealed_segments = BTreeMap::new();
    for seg_meta in &manifest.prepared.sealed_vlog_segments {
        let seg = VlogSegment::<IO>::open_at(
            seg_meta.segment_id,
            seg_meta.path.clone(),
            seg_meta.total_size,
            seg_meta.views.clone(),
            io_flags,
        )?;
        prepared_sealed_segments.insert(seg_meta.segment_id, seg);
    }

    let prep_active_path = base_dir.join(format!(
        "prep_vlog_{:04}.dat",
        manifest.prepared.active_segment_id
    ));
    let mut prep_active_vlog = VlogSegment::<IO>::open_at(
        manifest.prepared.active_segment_id,
        prep_active_path,
        manifest.prepared.active_write_offset,
        Vec::new(),
        io_flags,
    )?;
    prep_active_vlog.start_view(manifest.current_view);

    let prepared = VlogLsm::open_from_parts(
        "prep",
        base_dir,
        prep_active_vlog,
        prepared_sealed_segments,
        io_flags,
        manifest.prepared.next_segment_id,
        usize::MAX,
    );

    let mut state = TapirState {
        memtable: Memtable::new(),
        committed,
        prepared,
        prepare_cache: RefCell::new(PreparedTransactions::new(DEFAULT_PREPARE_CACHE_CAPACITY)),
        vlog_read_count: Cell::new(0),
        manifest,
        base_dir: base_dir.to_path_buf(),
        io_flags,
    };

    for seg_meta in state.manifest.committed.sealed_vlog_segments.clone() {
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
        state.manifest.committed.active_segment_id,
        state
            .base_dir
            .join(format!("vlog_seg_{:04}.dat", state.manifest.committed.active_segment_id)),
        state.manifest.committed.active_write_offset,
        Vec::new(),
        state.io_flags,
    )?;
    state.recover_segment(&active_for_recover)?;
    active_for_recover.close();

    for seg_meta in state.manifest.prepared.sealed_vlog_segments.clone() {
        let seg = VlogSegment::<IO>::open_at(
            seg_meta.segment_id,
            seg_meta.path,
            seg_meta.total_size,
            seg_meta.views,
            state.io_flags,
        )?;
        state.recover_prepared_segment(&seg)?;
        seg.close();
    }

    let prep_active_for_recover = VlogSegment::<IO>::open_at(
        state.manifest.prepared.active_segment_id,
        state
            .base_dir
            .join(format!(
                "prep_vlog_{:04}.dat",
                state.manifest.prepared.active_segment_id
            )),
        state.manifest.prepared.active_write_offset,
        Vec::new(),
        state.io_flags,
    )?;
    state.recover_prepared_segment(&prep_active_for_recover)?;
    prep_active_for_recover.close();

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
            .committed
            .segment_ref(segment_id)
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
        self.tapir.prepare(txn_id, transaction, commit_ts).unwrap();
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

    pub(crate) fn prepared_index_contains(&self, txn_id: &OccTransactionId) -> bool {
        self.tapir.prepared.index().contains_key(txn_id)
    }

    pub(crate) fn register_prepare_expect_conflict(
        &mut self,
        txn_id: OccTransactionId,
        transaction: &crate::occ::Transaction<String, String, Timestamp>,
        commit_ts: Timestamp,
    ) -> bool {
        self.tapir.prepare(txn_id, transaction, commit_ts).is_err()
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

        // After seal, InMemory entries are converted to OnDisk. Verify reads
        // go through the vlog + cache.
        let (value, _) = runner.tapir.do_uncommitted_get(&"k".to_string())
            .expect("get should succeed");
        assert_eq!(value.as_deref(), Some("v"));
        assert_eq!(runner.tapir.vlog_read_count.get(), 1);

        // Second read should hit the prepare_cache.
        let (value2, _) = runner.tapir.do_uncommitted_get(&"k".to_string())
            .expect("second get should succeed");
        assert_eq!(value2.as_deref(), Some("v"));
        assert_eq!(runner.tapir.vlog_read_count.get(), 1);
    }
}
