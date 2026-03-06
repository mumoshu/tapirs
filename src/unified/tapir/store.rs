use crate::mvcc::disk::error::StorageError;
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::memtable::{CompositeKey, MaxValue};
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::{Key, ShardNumber, Timestamp, Value};
use crate::unified::tapir::occ_cache::{MvccQueries, OccCache, OccCacheState, OccCachedTransaction};
use crate::unified::tapir::Transaction;
use crate::unified::tapir::{MvccIndexEntry, VlogSegment};
use crate::unified::wisckeylsm::lsm::{IndexMode, VlogLsm};
use crate::unified::wisckeylsm::manifest::UnifiedManifest;
use crate::unified::wisckeylsm::types::VlogPtr;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Entry stored in the prepared VlogLsm: commit timestamp + cross-shard transaction.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(bound(
    serialize = "K: serde::Serialize, V: serde::Serialize",
    deserialize = "K: serde::de::DeserializeOwned + Ord, V: serde::de::DeserializeOwned"
))]
pub(crate) struct PreparedEntry<K, V> {
    pub(crate) commit_ts: Timestamp,
    pub(crate) transaction: Arc<crate::occ::Transaction<K, V, Timestamp>>,
}

/// Borrow-splitting wrapper: holds only the MVCC VlogLsm reference,
/// allowing occ_cache to be mutably/immutably borrowed independently.
struct MvccView<'a, K: Ord, IO: DiskIo> {
    mvcc: &'a VlogLsm<CompositeKey<K, Timestamp>, MvccIndexEntry, IO>,
}

impl<K: Key + serde::Serialize + serde::de::DeserializeOwned, IO: DiskIo> MvccQueries<K>
    for MvccView<'_, K, IO>
{
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
            return Ok((Timestamp::default(), None));
        };

        // Find the next version (higher timestamp) for this key.
        // In BTreeMap order, higher timestamps come BEFORE lower ones
        // due to Reverse<Timestamp>. So we search from max_value and
        // iterate forward looking for a version with ts > at_ts.
        let start = CompositeKey::new(key.clone(), Timestamp::max_value());
        let mut next_ts = None;
        for (ck, _) in self.mvcc.memtable_range_from(&start) {
            if ck.key != *key { break; }
            if ck.timestamp.0 > at_ts {
                next_ts = Some(match next_ts {
                    Some(existing) => std::cmp::min(existing, ck.timestamp.0),
                    None => ck.timestamp.0,
                });
            }
        }
        for (ck, _) in self.mvcc.index_range(start..) {
            if ck.key != *key { break; }
            if ck.timestamp.0 > at_ts {
                next_ts = Some(match next_ts {
                    Some(existing) => std::cmp::min(existing, ck.timestamp.0),
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
            return Ok(entry
                .last_read_ts
                .map(|t| Timestamp { time: t, client_id: crate::IrClientId(0) }));
        }
        Ok(None)
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
            return Ok(entry
                .last_read_ts
                .map(|t| Timestamp { time: t, client_id: crate::IrClientId(0) }));
        }
        Ok(None)
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
            if ck.key > *end { break; }
            if ck.timestamp.0 > after_ts && ck.timestamp.0 < before_ts {
                return Ok(true);
            }
        }
        for (ck, _) in self.mvcc.index_range(from_ck..) {
            if ck.key > *end { break; }
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
            return Ok(ck.timestamp.0);
        }
        Ok(Timestamp::default())
    }
}

pub(crate) struct TapirState<K: Ord, V, IO: DiskIo> {
    /// Which shard this store serves. Used to extract shard-local read/write/scan
    /// sets from cross-shard transactions stored in the prepared VlogLsm.
    pub(crate) shard: ShardNumber,
    /// MVCC index: maps (key, timestamp) → MvccIndexEntry for get/get_at/scan.
    /// Each MvccIndexEntry stores (txn_id, write_index) resolved via committed VlogLsm.
    mvcc: VlogLsm<CompositeKey<K, Timestamp>, MvccIndexEntry, IO>,
    /// Committed transaction store: txn_id → Option<Transaction> in vlog.
    /// Some(txn) = committed at txn.commit_ts, None = aborted tombstone.
    /// On commit, the transaction is written here and MVCC entries point into it.
    committed: VlogLsm<OccTransactionId, Option<Arc<Transaction<K, V>>>, IO>,
    /// Prepared transaction store: txn_id → Option<cross-shard Transaction> in vlog.
    /// Some(txn) = actively prepared, None = tombstone (committed/aborted).
    /// Tombstones flow through seal to SSTs for durable removal on reopen.
    ///
    /// Stores the full cross-shard `occ::Transaction` (with `Sharded<K>` keys)
    /// rather than shard-local data. The backup coordinator needs cross-shard
    /// data to call `transaction.participants()` during `recover_coordination()`.
    ///
    // TODO: storing full cross-shard transactions duplicates data already in
    // IrRecord's VlogLsm and means OccCache rebuilding on open() reads more
    // data than necessary (it only needs shard-local read/write/scan sets).
    // Future: compact prepared entries once processed by coordinator, or store
    // shard-local data separately alongside the cross-shard vlog entries.
    prepared: VlogLsm<OccTransactionId, Option<PreparedEntry<K, V>>, IO>,
    /// In-memory OCC conflict detection state for prepared transactions.
    occ_cache: OccCache<K>,
    /// Persisted metadata: view number, vlog segment positions, SST lists.
    manifest: UnifiedManifest,
    /// Root directory for all store files (vlogs, SSTs, manifest).
    base_dir: PathBuf,
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

const RAW_ENTRY_OVERHEAD: u32 = 25;
const TAPIR_COMMITTED_TXN_ENTRY_TYPE: u8 = 0x80;
const TAPIR_PREPARED_TXN_ENTRY_TYPE: u8 = 0x81;
const TAPIR_MVCC_ENTRY_TYPE: u8 = 0x82;

impl<K: Ord + Clone, V, IO: DiskIo> TapirState<K, V, IO> {
    fn insert_mvcc_entry(
        &mut self,
        key: K,
        ts: Timestamp,
        txn_id: OccTransactionId,
        write_index: u16,
    ) {
        let ck = CompositeKey::new(key, ts);
        self.mvcc.put(ck, MvccIndexEntry { txn_id, write_index, last_read_ts: None });
    }

    fn update_mvcc_last_read_ts(&mut self, key: &K, read: Timestamp, commit_time: u64)
    where
        K: serde::Serialize + serde::de::DeserializeOwned,
    {
        let search = CompositeKey::new(key.clone(), read);
        if let Ok(Some((found_ck, mut entry))) = self.mvcc.range_get_first(&search)
            && found_ck.key == *key
        {
            entry.last_read_ts = Some(match entry.last_read_ts {
                Some(existing) => existing.max(commit_time),
                None => commit_time,
            });
            self.mvcc.put(found_ck, entry);
        }
    }

    pub(crate) fn abort(&mut self, txn_id: &OccTransactionId)
    where
        K: Key + serde::Serialize + serde::de::DeserializeOwned,
        V: Value + serde::Serialize + serde::de::DeserializeOwned,
    {
        // Look up the prepared transaction to get its read/write sets for cache cleanup.
        if let Ok(Some(Some(entry))) = self.prepared.get(txn_id) {
            let shard = self.shard;
            let read_set: Vec<(K, Timestamp)> = entry
                .transaction
                .shard_read_set(shard)
                .map(|(k, ts)| (k.clone(), ts))
                .collect();
            let write_set: Vec<(K, Option<V>)> = entry
                .transaction
                .shard_write_set(shard)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            self.occ_cache
                .unregister_prepare(&read_set, &write_set, entry.commit_ts);
        }
        self.prepared.put(*txn_id, None);
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
        self.occ_cache
            .unregister_prepare(read_set, write_set, commit);
        self.prepared.put(txn_id, None);

        let arc_txn = Arc::new(Transaction {
            transaction_id: txn_id,
            commit_ts: commit,
            read_set: read_set.to_vec(),
            write_set: write_set.to_vec(),
            scan_set: scan_set.to_vec(),
        });

        self.committed.put(txn_id, Some(arc_txn));

        for (i, (key, _value)) in write_set.iter().enumerate() {
            self.insert_mvcc_entry(key.clone(), commit, txn_id, i as u16);
        }

        for (key, read) in read_set {
            self.update_mvcc_last_read_ts(key, *read, commit.time);
        }

        Ok(())
    }

    pub(crate) fn prepare(
        &mut self,
        txn_id: OccTransactionId,
        transaction: &crate::occ::Transaction<K, V, Timestamp>,
        commit_ts: Timestamp,
    ) -> Result<crate::occ::PrepareResult<Timestamp>, StorageError>
    where
        K: Key + serde::Serialize + serde::de::DeserializeOwned,
        V: Value + serde::Serialize + serde::de::DeserializeOwned,
    {
        let read_set: Vec<(K, Timestamp)> = transaction
            .shard_read_set(self.shard)
            .map(|(k, ts)| (k.clone(), ts))
            .collect();

        let write_set: Vec<(K, Option<V>)> = transaction
            .shard_write_set(self.shard)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let scan_set: Vec<(K, K, Timestamp)> = transaction
            .shard_scan_set(self.shard)
            .map(|entry| (entry.start_key.clone(), entry.end_key.clone(), entry.timestamp))
            .collect();

        let mvcc_view = MvccView { mvcc: &self.mvcc };
        let result = self
            .occ_cache
            .occ_check(&mvcc_view, &read_set, &write_set, &scan_set, commit_ts)?;

        if !result.is_ok() {
            return Ok(result);
        }

        self.occ_cache
            .register_prepare(&read_set, &write_set, commit_ts);
        // TODO: compact prepared entries once processed by coordinator.
        let entry = PreparedEntry {
            commit_ts,
            transaction: Arc::new(transaction.clone()),
        };
        self.prepared.put(txn_id, Some(entry));
        Ok(crate::occ::PrepareResult::Ok)
    }

    fn resolve_value(&self, entry: &MvccIndexEntry) -> Result<Option<V>, StorageError>
    where
        K: Key + serde::Serialize + serde::de::DeserializeOwned,
        V: Value + serde::Serialize + serde::de::DeserializeOwned,
    {
        if let Some(Some(txn)) = self.committed.get(&entry.txn_id)? {
            Ok(txn.write_set.get(entry.write_index as usize).and_then(|(_, v)| v.clone()))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn seal_current_view(&mut self, min_view_vlog_size: u64) -> Result<(), StorageError>
    where
        K: serde::Serialize + serde::de::DeserializeOwned,
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

        let mvcc_sealed =
            self.mvcc
                .seal_view(min_view_vlog_size, |_ck, _entry| {
                    Some((TAPIR_MVCC_ENTRY_TYPE, 0, 0))
                })?;
        if let Some(meta) = mvcc_sealed {
            self.manifest.mvcc.sealed_vlog_segments.push(meta);
        }

        self.manifest.max_read_time = self.occ_cache.max_read_time().map(|ts| ts.time);
        self.manifest.current_view += 1;
        self.manifest.committed.active_segment_id = self.committed.active_vlog_id();
        self.manifest.committed.active_write_offset = self.committed.active_write_offset();
        self.manifest.committed.next_segment_id = self.committed.next_segment_id();
        self.manifest.committed.sst_metas = self.committed.sst_metas().to_vec();
        self.manifest.committed.next_sst_id = self.committed.next_sst_id();
        self.manifest.prepared.active_segment_id = self.prepared.active_vlog_id();
        self.manifest.prepared.active_write_offset = self.prepared.active_write_offset();
        self.manifest.prepared.next_segment_id = self.prepared.next_segment_id();
        self.manifest.prepared.sst_metas = self.prepared.sst_metas().to_vec();
        self.manifest.prepared.next_sst_id = self.prepared.next_sst_id();
        self.manifest.mvcc.active_segment_id = self.mvcc.active_vlog_id();
        self.manifest.mvcc.active_write_offset = self.mvcc.active_write_offset();
        self.manifest.mvcc.next_segment_id = self.mvcc.next_segment_id();
        self.manifest.mvcc.sst_metas = self.mvcc.sst_metas().to_vec();
        self.manifest.mvcc.next_sst_id = self.mvcc.next_sst_id();
        self.manifest.save::<IO>(&self.base_dir)?;
        self.committed.start_view(self.manifest.current_view);
        self.prepared.start_view(self.manifest.current_view);
        self.mvcc.start_view(self.manifest.current_view);
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

fn read_committed_entry_from_segment<
    K: serde::de::DeserializeOwned,
    V: serde::de::DeserializeOwned,
    IO: DiskIo,
>(
    segment: &VlogSegment<IO>,
    txn_ptr: &VlogPtr,
) -> Result<Option<Arc<Transaction<K, V>>>, StorageError> {
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
        if let Some(txn_arc) = read_committed_entry_from_segment::<K, V, IO>(segment, &ptr)? {
            // Arc is freshly deserialized — refcount is always 1.
            let txn = Arc::try_unwrap(txn_arc).ok().expect("freshly deserialized Arc has refcount 1");
            out.push((offset, txn));
        }
    }
    Ok(out)
}

pub(crate) fn open<K: Key, V: Value, IO: DiskIo>(
    base_dir: &Path,
    io_flags: OpenFlags,
    shard: ShardNumber,
    _linearizable: bool,
) -> Result<TapirState<K, V, IO>, StorageError> {
    IO::create_dir_all(base_dir)?;

    let manifest = match UnifiedManifest::load::<IO>(base_dir) {
        Ok(Some(m)) => m,
        Ok(None) => UnifiedManifest::new(),
        Err(_) => {
            // Old manifest format without mvcc field — start fresh
            UnifiedManifest::new()
        }
    };

    let committed = VlogLsm::open_from_manifest(
        "",
        base_dir,
        &manifest.committed,
        manifest.current_view,
        io_flags,
        IndexMode::SstOnly,
    )?;
    let prepared = VlogLsm::open_from_manifest(
        "prep",
        base_dir,
        &manifest.prepared,
        manifest.current_view,
        io_flags,
        IndexMode::InMemory,
    )?;
    let mvcc = VlogLsm::open_from_manifest(
        "mvcc",
        base_dir,
        &manifest.mvcc,
        manifest.current_view,
        io_flags,
        IndexMode::InMemory,
    )?;

    // Rebuild OccCache from prepared VlogLsm + manifest's max_read_time.
    // Iterate index to find all txn_ids, get() each to filter active prepares.
    let mut recovered_txns = Vec::new();
    let index_keys: Vec<OccTransactionId> = prepared
        .index_range(..)
        .map(|(k, _)| *k)
        .collect();
    for txn_id in index_keys {
        let vlog_entry: Option<Option<PreparedEntry<K, V>>> = prepared.get(&txn_id)?;
        if let Some(Some(entry)) = vlog_entry {
            let read_set: Vec<(K, Timestamp)> = entry
                .transaction
                .shard_read_set(shard)
                .map(|(k, ts)| (k.clone(), ts))
                .collect();
            let write_keys: Vec<K> = entry
                .transaction
                .shard_write_set(shard)
                .map(|(k, _v)| k.clone())
                .collect();
            recovered_txns.push(OccCachedTransaction {
                read_set,
                write_set: write_keys,
                commit_ts: entry.commit_ts,
            });
        }
    }

    let recovered_max_read_time = manifest.max_read_time.map(|time| Timestamp {
        time,
        client_id: crate::IrClientId(0),
    });

    let occ_state = if recovered_txns.is_empty() && recovered_max_read_time.is_none() {
        None
    } else {
        Some(OccCacheState {
            transactions: recovered_txns,
            max_read_time: recovered_max_read_time,
        })
    };

    let occ_cache = OccCache::new(true, occ_state);

    let state = TapirState {
        shard,
        mvcc,
        committed,
        prepared,
        occ_cache,
        manifest,
        base_dir: base_dir.to_path_buf(),
    };

    Ok(state)
}

impl<
        K: Key + serde::Serialize + serde::de::DeserializeOwned,
        V: Value + serde::Serialize + serde::de::DeserializeOwned,
        IO: DiskIo,
    > TapirState<K, V, IO>
{
    pub(crate) fn snapshot_get(&self, key: &K) -> Result<(Option<V>, Timestamp), StorageError> {
        let search = CompositeKey::new(key.clone(), Timestamp::max_value());
        if let Some((ck, entry)) = self.mvcc.range_get_first(&search)?
            && ck.key == *key
        {
            return Ok((self.resolve_value(&entry)?, ck.timestamp.0));
        }
        Ok((None, Timestamp::default()))
    }

    pub(crate) fn snapshot_get_at(&self, key: &K, ts: Timestamp) -> Result<(Option<V>, Timestamp), StorageError> {
        let search = CompositeKey::new(key.clone(), ts);
        if let Some((ck, entry)) = self.mvcc.range_get_first(&search)?
            && ck.key == *key
        {
            return Ok((self.resolve_value(&entry)?, ck.timestamp.0));
        }
        Ok((None, Timestamp::default()))
    }

    pub(crate) fn snapshot_scan(
        &self,
        start: &K,
        end: &K,
        ts: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, StorageError> {
        let from_ck = CompositeKey::new(start.clone(), Timestamp::max_value());

        // Collect all entries: index (older, sealed) then memtable (newer, overrides)
        let mut merged: BTreeMap<CompositeKey<K, Timestamp>, MvccIndexEntry> = BTreeMap::new();
        for (ck, ptr) in self.mvcc.index_range(from_ck.clone()..) {
            if ck.key > *end { break; }
            let entry: MvccIndexEntry = self.mvcc.read_value_from_vlog(ptr)?;
            merged.insert(ck.clone(), entry);
        }
        for (ck, entry) in self.mvcc.memtable_range_from(&from_ck) {
            if ck.key > *end { break; }
            merged.insert(ck.clone(), entry.clone());
        }

        // Filter by timestamp, dedup by user key (first = latest due to CompositeKey ordering)
        let mut out = Vec::new();
        let mut last_key: Option<&K> = None;
        for (ck, entry) in &merged {
            if ck.timestamp.0 > ts { continue; }
            if last_key == Some(&ck.key) { continue; }
            last_key = Some(&ck.key);
            let value = self.resolve_value(entry)?;
            out.push((ck.key.clone(), value, ck.timestamp.0));
        }
        Ok(out)
    }
}

#[cfg(test)]
impl<
        K: Key + serde::Serialize + serde::de::DeserializeOwned,
        V: Value + serde::Serialize + serde::de::DeserializeOwned,
        IO: DiskIo,
    > TapirState<K, V, IO>
{
    /// Read a key with RO transaction protection.
    /// Returns PrepareConflict if a prepared write to this key at ts <= snapshot_ts
    /// is in flight. On success, if linearizable is true, sets last_read_ts on the
    /// MVCC entry (for existing keys) or records a degenerate range_read (for
    /// non-existent keys, preventing phantom writes).
    pub(crate) fn snapshot_get_protected(
        &mut self,
        key: &K,
        snapshot_ts: Timestamp,
    ) -> Result<(Option<V>, Timestamp), crate::occ::PrepareConflict> {
        if self.occ_cache.check_get_conflict(key, snapshot_ts) {
            return Err(crate::occ::PrepareConflict);
        }

        let (value, ts) = self
            .snapshot_get_at(key, snapshot_ts)
            .map_err(|_| crate::occ::PrepareConflict)?;

        if value.is_some() {
            self.update_mvcc_last_read_ts(key, snapshot_ts, snapshot_ts.time);
        } else {
            self.occ_cache
                .record_range_read(key.clone(), key.clone(), snapshot_ts);
        }
        self.occ_cache.update_max_read_time(snapshot_ts);

        Ok((value, ts))
    }

    /// Scan a range with RO transaction protection.
    /// Returns PrepareConflict if a prepared write in [start, end] at ts <= snapshot_ts
    /// is in flight. On success, if linearizable is true, records a range_read to
    /// prevent future prepares from writing into this range at ts < snapshot_ts.
    pub(crate) fn snapshot_scan_protected(
        &mut self,
        start: &K,
        end: &K,
        snapshot_ts: Timestamp,
    ) -> Result<Vec<(K, Option<V>, Timestamp)>, crate::occ::PrepareConflict> {
        if self.occ_cache.check_scan_conflict(start, end, snapshot_ts) {
            return Err(crate::occ::PrepareConflict);
        }

        let results = self
            .snapshot_scan(start, end, snapshot_ts)
            .map_err(|_| crate::occ::PrepareConflict)?;

        self.occ_cache
            .record_range_read(start.clone(), end.clone(), snapshot_ts);
        self.occ_cache.update_max_read_time(snapshot_ts);

        Ok(results)
    }
}

#[cfg(test)]
pub(crate) struct TapirStateRunner {
    state: TapirState<String, String, crate::mvcc::disk::memory_io::MemoryIo>,
    base_dir: PathBuf,
}

#[cfg(test)]
impl TapirStateRunner {
    pub(crate) fn open(base_dir: PathBuf, io_flags: OpenFlags) -> Result<Self, StorageError> {
        crate::mvcc::disk::memory_io::MemoryIo::create_dir_all(&base_dir)?;
        let state = open::<String, String, crate::mvcc::disk::memory_io::MemoryIo>(
            &base_dir,
            io_flags,
            ShardNumber(0),
            true,
        )?;
        Ok(Self {
            state,
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

    pub(crate) fn prepare(
        &mut self,
        txn_id: OccTransactionId,
        transaction: &crate::occ::Transaction<String, String, Timestamp>,
        commit_ts: Timestamp,
    ) {
        let result = self.state.prepare(txn_id, transaction, commit_ts).unwrap();
        assert!(
            result.is_ok(),
            "prepare expected Ok, got {result:?}"
        );
    }

    pub(crate) fn commit(
        &mut self,
        txn_id: OccTransactionId,
        read_set: &[(String, Timestamp)],
        write_set: &[(String, Option<String>)],
        scan_set: &[(String, String, Timestamp)],
        commit_ts: Timestamp,
    ) -> Result<(), StorageError> {
        self.state
            .commit(txn_id, read_set, write_set, scan_set, commit_ts)
    }

    pub(crate) fn seal(&mut self, min_view_vlog_size: u64) -> Result<(), StorageError> {
        self.state.seal_current_view(min_view_vlog_size)
    }

    pub(crate) fn get_at(
        &self,
        key: &str,
        ts: Timestamp,
    ) -> Result<(Option<String>, Timestamp), StorageError> {
        self.state.snapshot_get_at(&key.to_string(), ts)
    }

    pub(crate) fn scan(
        &self,
        start: &str,
        end: &str,
        ts: Timestamp,
    ) -> Result<Vec<(String, Option<String>, Timestamp)>, StorageError> {
        self.state
            .snapshot_scan(&start.to_string(), &end.to_string(), ts)
    }

    pub(crate) fn prepared_index_contains(&self, txn_id: &OccTransactionId) -> bool {
        // get() checks memtable then index. Some(Some(_)) = active, Some(None) = tombstone.
        matches!(self.state.prepared.get(txn_id), Ok(Some(Some(_))))
    }

    pub(crate) fn prepare_expect_conflict(
        &mut self,
        txn_id: OccTransactionId,
        transaction: &crate::occ::Transaction<String, String, Timestamp>,
        commit_ts: Timestamp,
    ) -> bool {
        let result = self.state.prepare(txn_id, transaction, commit_ts).unwrap();
        !result.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IrClientId;

    #[test]
    fn commit_and_get_basic() {
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
            .state
            .commit(
                txn_id,
                &[("r".to_string(), ts)],
                &[("k".to_string(), Some("v".to_string()))],
                &[],
                ts,
            )
        .expect("commit should append committed txn");

        let (value, _) = runner.state.snapshot_get(&"k".to_string())
            .expect("get should succeed");
        assert_eq!(value.as_deref(), Some("v"));

        // After seal, value should still be readable via committed VlogLsm
        runner.seal(0).expect("seal should finalize current view");

        let (value2, _) = runner.state.snapshot_get(&"k".to_string())
            .expect("get after seal should succeed");
        assert_eq!(value2.as_deref(), Some("v"));
    }

    fn test_ts(time: u64) -> Timestamp {
        Timestamp {
            time,
            client_id: IrClientId(1),
        }
    }

    fn test_txn_id(client: u64, num: u64) -> OccTransactionId {
        OccTransactionId {
            client_id: IrClientId(client),
            number: num,
        }
    }

    fn make_write_only_txn(
        writes: Vec<(&str, Option<&str>)>,
    ) -> crate::occ::Transaction<String, String, Timestamp> {
        let mut txn = crate::occ::Transaction::<String, String, Timestamp>::default();
        for (key, value) in writes {
            txn.add_write(
                crate::tapir::Sharded {
                    shard: crate::tapir::ShardNumber(0),
                    key: key.to_string(),
                },
                value.map(|v| v.to_string()),
            );
        }
        txn
    }

    fn open_test_state() -> TapirState<String, String, crate::mvcc::disk::memory_io::MemoryIo> {
        let base_dir = crate::mvcc::disk::memory_io::MemoryIo::temp_path();
        open(&base_dir, OpenFlags { create: true, direct: false }, ShardNumber(0), true).unwrap()
    }

    #[test]
    fn snapshot_get_protected_sets_last_read_ts() {
        let mut state = open_test_state();

        // Commit a key
        state
            .commit(
                test_txn_id(1, 1),
                &[],
                &[("x".to_string(), Some("v1".to_string()))],
                &[],
                test_ts(5),
            )
            .unwrap();

        // Protected read at ts=10
        let (value, ts) = state
            .snapshot_get_protected(&"x".to_string(), test_ts(10))
            .unwrap();
        assert_eq!(value.as_deref(), Some("v1"));
        assert_eq!(ts, test_ts(5));

        // Now prepare a write to "x" at ts=8 → should be rejected (Retry)
        // because last_read_ts=10 > commit_ts=8
        let txn = make_write_only_txn(vec![("x", Some("v2"))]);
        let result = state.prepare(test_txn_id(2, 1), &txn, test_ts(8)).unwrap();
        assert!(
            result.is_retry(),
            "prepare at ts=8 after protected read at ts=10 should Retry, got {result:?}"
        );
    }

    #[test]
    fn snapshot_scan_protected_prevents_write_in_range() {
        let mut state = open_test_state();

        // Commit a key in range
        state
            .commit(
                test_txn_id(1, 1),
                &[],
                &[("m".to_string(), Some("v1".to_string()))],
                &[],
                test_ts(5),
            )
            .unwrap();

        // Protected scan ["a", "z"] at ts=10
        let results = state
            .snapshot_scan_protected(
                &"a".to_string(),
                &"z".to_string(),
                test_ts(10),
            )
            .unwrap();
        assert_eq!(results.len(), 1);

        // Prepare a write to "n" (in range) at ts=8 → should Retry
        // because range_reads has ("a", "z", ts=10) and ts=10 > ts=8
        let txn = make_write_only_txn(vec![("n", Some("v2"))]);
        let result = state.prepare(test_txn_id(2, 1), &txn, test_ts(8)).unwrap();
        assert!(
            result.is_retry(),
            "prepare at ts=8 in protected range should Retry, got {result:?}"
        );
    }

    #[test]
    fn snapshot_get_protected_phantom_prevention() {
        let mut state = open_test_state();

        // Protected read of non-existent key records degenerate range_read
        let (value, _) = state
            .snapshot_get_protected(&"phantom".to_string(), test_ts(10))
            .unwrap();
        assert!(value.is_none());

        // Prepare a write to "phantom" at ts=8 → should Retry
        let txn = make_write_only_txn(vec![("phantom", Some("v1"))]);
        let result = state.prepare(test_txn_id(1, 1), &txn, test_ts(8)).unwrap();
        assert!(
            result.is_retry(),
            "prepare at ts=8 after phantom read at ts=10 should Retry, got {result:?}"
        );
    }

    #[test]
    fn snapshot_get_protected_conflict_with_prepared_write() {
        let mut state = open_test_state();

        // Commit a key
        state
            .commit(
                test_txn_id(1, 1),
                &[],
                &[("x".to_string(), Some("v1".to_string()))],
                &[],
                test_ts(5),
            )
            .unwrap();

        // Prepare a write to "x" at ts=8
        let txn = make_write_only_txn(vec![("x", Some("v2"))]);
        let result = state.prepare(test_txn_id(2, 1), &txn, test_ts(8)).unwrap();
        assert!(result.is_ok());

        // Protected read of "x" at ts=10 → should fail (PrepareConflict)
        // because prepared write at ts=8 <= snapshot_ts=10
        let read_result = state.snapshot_get_protected(&"x".to_string(), test_ts(10));
        assert!(
            read_result.is_err(),
            "protected read should fail when prepared write exists at ts <= snapshot_ts"
        );
    }

    #[test]
    fn recovery_rebuilds_occ_cache_from_prepared() {
        let base_dir = crate::mvcc::disk::memory_io::MemoryIo::temp_path();
        let flags = OpenFlags { create: true, direct: false };

        // Phase 1: prepare a transaction, seal, then "crash" (drop state)
        {
            let mut state: TapirState<String, String, crate::mvcc::disk::memory_io::MemoryIo> =
                open(&base_dir, flags, ShardNumber(0), true).unwrap();

            let txn = make_write_only_txn(vec![("x", Some("v1"))]);
            let result = state.prepare(test_txn_id(1, 1), &txn, test_ts(10)).unwrap();
            assert!(result.is_ok());

            state.seal_current_view(0).unwrap();
        }

        // Phase 2: reopen — OccCache should be rebuilt from prepared VlogLsm
        {
            let mut state: TapirState<String, String, crate::mvcc::disk::memory_io::MemoryIo> =
                open(&base_dir, flags, ShardNumber(0), true).unwrap();

            // Another prepare writing to the same key "x" should conflict
            // because the recovered OccCache has "x" registered as a prepared write
            let txn2 = make_write_only_txn(vec![("x", Some("v2"))]);
            let result = state.prepare(test_txn_id(2, 1), &txn2, test_ts(15)).unwrap();
            assert!(
                !result.is_ok(),
                "prepare to same key as recovered prepared txn should conflict, got {result:?}"
            );
        }
    }

    #[test]
    fn recovery_max_read_time_blocks_low_ts_prepare() {
        let base_dir = crate::mvcc::disk::memory_io::MemoryIo::temp_path();
        let flags = OpenFlags { create: true, direct: false };

        // Phase 1: protected read (sets max_read_time), seal, "crash"
        {
            let mut state: TapirState<String, String, crate::mvcc::disk::memory_io::MemoryIo> =
                open(&base_dir, flags, ShardNumber(0), true).unwrap();

            // Commit a key, then do a protected read at ts=20
            state
                .commit(
                    test_txn_id(1, 1),
                    &[],
                    &[("k".to_string(), Some("v".to_string()))],
                    &[],
                    test_ts(5),
                )
                .unwrap();
            let _ = state
                .snapshot_get_protected(&"k".to_string(), test_ts(20))
                .unwrap();

            state.seal_current_view(0).unwrap();
        }

        // Phase 2: reopen — max_read_time=20 should block prepares at ts < 20
        {
            let mut state: TapirState<String, String, crate::mvcc::disk::memory_io::MemoryIo> =
                open(&base_dir, flags, ShardNumber(0), true).unwrap();

            let txn = make_write_only_txn(vec![("any_key", Some("v2"))]);
            let result = state.prepare(test_txn_id(2, 1), &txn, test_ts(15)).unwrap();
            assert!(
                result.is_retry(),
                "prepare at ts=15 after recovered max_read_time=20 should Retry, got {result:?}"
            );

            // But prepare at ts=25 should succeed (above max_read_time)
            let txn3 = make_write_only_txn(vec![("any_key", Some("v3"))]);
            let result3 = state.prepare(test_txn_id(3, 1), &txn3, test_ts(25)).unwrap();
            assert!(
                result3.is_ok(),
                "prepare at ts=25 above max_read_time=20 should Ok, got {result3:?}"
            );
        }
    }
}
