use crate::mvcc::disk::error::StorageError;
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::memtable::{CompositeKey, MaxValue};
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::{Key, Timestamp, Value};
use crate::unified::tapir::Transaction;
use crate::unified::tapir::{MvccIndexEntry, VlogSegment};
use crate::unified::wisckeylsm::lsm::VlogLsm;
use crate::unified::wisckeylsm::manifest::UnifiedManifest;
use crate::unified::wisckeylsm::types::VlogPtr;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) struct TapirState<K: Ord, V, IO: DiskIo> {
    /// MVCC index: maps (key, timestamp) → MvccIndexEntry for get/get_at/scan.
    /// Each MvccIndexEntry stores (txn_id, write_index) resolved via committed VlogLsm.
    mvcc: VlogLsm<CompositeKey<K, Timestamp>, MvccIndexEntry, IO>,
    /// Committed transaction store: txn_id → serialized Transaction in vlog.
    /// On commit, the transaction is written here and MVCC entries point into it.
    committed: VlogLsm<OccTransactionId, Arc<Transaction<K, V>>, IO>,
    /// Prepared transaction store: txn_id → Option<Transaction> in vlog.
    /// Some(txn) = actively prepared, None = tombstone (committed/aborted).
    /// Tombstones flow through seal to SSTs for durable removal on reopen.
    prepared: VlogLsm<OccTransactionId, Option<Arc<Transaction<K, V>>>, IO>,
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

    pub(crate) fn abort(&mut self, txn_id: &OccTransactionId) {
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
        self.prepared.put(txn_id, None);

        let arc_txn = Arc::new(Transaction {
            transaction_id: txn_id,
            commit_ts: commit,
            read_set: read_set.to_vec(),
            write_set: write_set.to_vec(),
            scan_set: scan_set.to_vec(),
        });

        self.committed.put(txn_id, arc_txn);

        for (i, (key, _value)) in write_set.iter().enumerate() {
            self.insert_mvcc_entry(key.clone(), commit, txn_id, i as u16);
        }

        for (key, read) in read_set {
            self.update_mvcc_last_read_ts(key, *read, commit.time);
        }

        Ok(())
    }

    fn has_conflicting_prepare(
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

        // Check memtable entries (current view's prepared txns, skip tombstones)
        for txn in self.prepared.memtable_values().flatten() {
            for (key, _) in &txn.write_set {
                if new_keys.contains(key) {
                    return Ok(true);
                }
            }
        }

        // Check index entries (sealed views' prepared txns, loaded from vlog)
        for txn_id in self.prepared.index().keys() {
            if let Some(Some(txn)) = self.prepared.get(txn_id)? {
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

        if self.has_conflicting_prepare(&write_set)? {
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

        self.prepared.put(txn_id, Some(prepare));
        Ok(())
    }

    fn resolve_value(&self, entry: &MvccIndexEntry) -> Result<Option<V>, StorageError>
    where
        K: Key + serde::Serialize + serde::de::DeserializeOwned,
        V: Value + serde::Serialize + serde::de::DeserializeOwned,
    {
        if let Some(txn) = self.committed.get(&entry.txn_id)? {
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
    )?;
    let prepared = VlogLsm::open_from_manifest(
        "prep",
        base_dir,
        &manifest.prepared,
        manifest.current_view,
        io_flags,
    )?;
    let mvcc = VlogLsm::open_from_manifest(
        "mvcc",
        base_dir,
        &manifest.mvcc,
        manifest.current_view,
        io_flags,
    )?;

    let state = TapirState {
        mvcc,
        committed,
        prepared,
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
        for (ck, entry) in self.mvcc.memtable_range(from_ck..) {
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
pub(crate) struct TapirStateRunner {
    state: TapirState<String, String, crate::mvcc::disk::memory_io::MemoryIo>,
    base_dir: PathBuf,
}

#[cfg(test)]
impl TapirStateRunner {
    pub(crate) fn open(base_dir: PathBuf, io_flags: OpenFlags) -> Result<Self, StorageError> {
        crate::mvcc::disk::memory_io::MemoryIo::create_dir_all(&base_dir)?;
        let state = open::<String, String, crate::mvcc::disk::memory_io::MemoryIo>(&base_dir, io_flags)?;
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
        self.state.prepare(txn_id, transaction, commit_ts).unwrap();
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
        self.state.prepare(txn_id, transaction, commit_ts).is_err()
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
}
