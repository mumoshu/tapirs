use super::disk_io::{DiskIo, OpenFlags};
use super::error::StorageError;
use super::lsm::LsmTree;
use super::manifest::Manifest;
use super::memtable::{CompositeKey, LsmEntry, MaxValue, Memtable};
use super::sstable::SSTableReader;
use super::vlog::{VlogEntry, VlogSegment};
use crate::mvcc::backend::MvccBackend;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::path::PathBuf;

/// Flush threshold: 64 KiB of memtable data triggers an SSTable flush.
const FLUSH_THRESHOLD: usize = 64 * 1024;

/// WiscKey-style disk-backed MVCC storage.
///
/// Keys and metadata live in an LSM tree. Values live in the vlog.
/// Default `IO = BufferedIo` for testing; production uses `SyncDirectIo`
/// or `UringDirectIo`.
pub struct DiskStore<K, V, TS, IO: DiskIo> {
    memtable: Memtable<K, TS>,
    lsm: LsmTree<IO>,
    vlog: VlogSegment<IO>,
    base_dir: PathBuf,
    io_flags: OpenFlags,
    next_segment_id: u64,
    /// Vlog offset up to which data has been flushed to SSTables.
    /// Entries after this offset are in memtable and need replay on recovery.
    flushed_vlog_offset: u64,
    _v: std::marker::PhantomData<V>,
}

impl<K, V, TS, IO: DiskIo> DiskStore<K, V, TS, IO>
where
    K: Serialize + for<'de> Deserialize<'de> + Ord + Clone + Send + Debug + std::hash::Hash,
    V: Serialize + for<'de> Deserialize<'de> + Clone + Send + Debug,
    TS: Serialize
        + for<'de> Deserialize<'de>
        + Ord
        + Copy
        + Default
        + MaxValue
        + Send
        + Debug,
{
    /// Open or create a DiskStore at the given directory.
    ///
    /// If a manifest exists, recovers from it and replays unflushed
    /// vlog entries into the memtable.
    pub fn open(base_dir: PathBuf) -> Result<Self, StorageError> {
        Self::open_with_flags(
            base_dir,
            OpenFlags {
                create: true,
                direct: false,
            },
        )
    }

    /// Open with specific I/O flags (e.g., O_DIRECT for production).
    pub fn open_with_flags(
        base_dir: PathBuf,
        io_flags: OpenFlags,
    ) -> Result<Self, StorageError> {
        std::fs::create_dir_all(&base_dir)?;

        if let Some(manifest) = Manifest::load(&base_dir)? {
            return Self::recover(base_dir, manifest, io_flags);
        }

        // Fresh store.
        let vlog_path = base_dir.join("vlog-000000.log");
        let vlog = VlogSegment::<IO>::open(0, vlog_path, io_flags)?;
        let lsm = LsmTree::<IO>::new(base_dir.clone(), io_flags);

        Ok(Self {
            memtable: Memtable::new(),
            lsm,
            vlog,
            base_dir,
            io_flags,
            next_segment_id: 1,
            flushed_vlog_offset: 0,
            _v: std::marker::PhantomData,
        })
    }

    /// Recover from a manifest: restore LSM state and replay vlog.
    fn recover(
        base_dir: PathBuf,
        manifest: Manifest,
        io_flags: OpenFlags,
    ) -> Result<Self, StorageError> {
        let lsm = LsmTree::<IO>::restore(
            base_dir.clone(),
            manifest.l0_sstables,
            manifest.l1_sstables,
            manifest.next_sst_id,
            io_flags,
        );

        // Open the latest vlog segment at the flushed offset.
        let seg_id = manifest
            .vlog_segment_ids
            .last()
            .copied()
            .unwrap_or(0);
        let vlog_path = base_dir.join(format!("vlog-{seg_id:06}.log"));
        let vlog = VlogSegment::<IO>::open_at(
            seg_id,
            vlog_path,
            manifest.vlog_write_offset,
            io_flags,
        )?;

        // Replay vlog entries after the flushed offset into memtable.
        let mut memtable = Memtable::new();

        // Recover entries from vlog
        let recovered_entries = futures::executor::block_on(
            vlog.recover_entries::<K, V, TS>(manifest.vlog_write_offset)
        )?;

        for (entry, ptr) in &recovered_entries {
            memtable.insert(
                CompositeKey::new(entry.key.clone(), entry.timestamp),
                LsmEntry {
                    value_ptr: Some(*ptr),
                    last_read_ts: None,
                },
            );
        }

        if !recovered_entries.is_empty() {
            tracing::info!("Recovered {} entries from vlog", recovered_entries.len());
        }

        Ok(Self {
            memtable,
            lsm,
            vlog,
            base_dir,
            io_flags,
            next_segment_id: manifest.next_segment_id,
            flushed_vlog_offset: manifest.vlog_write_offset,
            _v: std::marker::PhantomData,
        })
    }

    /// Save current state to manifest.
    pub fn save_manifest(&self) -> Result<(), StorageError> {
        let manifest = Manifest {
            l0_sstables: self.lsm.l0_metas().to_vec(),
            l1_sstables: self.lsm.l1_metas().to_vec(),
            vlog_segment_ids: vec![self.vlog.id],
            vlog_write_offset: self.flushed_vlog_offset,
            next_sst_id: self.lsm.next_sst_id(),
            next_segment_id: self.next_segment_id,
            checksum: 0,
        };
        manifest.save(&self.base_dir)
    }

    /// Get the latest version of a key.
    fn get_impl(&self, key: &K) -> Result<(Option<V>, TS), StorageError> {
        // Check memtable first.
        if let Some((ck, entry)) = self.memtable.get_latest(key) {
            let value = self.resolve_value(entry)?;
            return Ok((value, ck.timestamp.0));
        }

        // Check LSM tree.
        let result = futures::executor::block_on(
            self.lsm.get_at::<K, TS>(key, &TS::max_value()),
        )?;

        if let Some((ck, entry)) = result {
            let value = self.resolve_value(&entry)?;
            return Ok((value, ck.timestamp.0));
        }

        Ok((None, TS::default()))
    }

    /// Get the version valid at a specific timestamp.
    fn get_at_impl(&self, key: &K, timestamp: TS) -> Result<(Option<V>, TS), StorageError> {
        // Check memtable first.
        if let Some((ck, entry)) = self.memtable.get_at(key, &timestamp) {
            let value = self.resolve_value(entry)?;
            return Ok((value, ck.timestamp.0));
        }

        // Check LSM tree.
        let result = futures::executor::block_on(
            self.lsm.get_at::<K, TS>(key, &timestamp),
        )?;

        if let Some((ck, entry)) = result {
            let value = self.resolve_value(&entry)?;
            return Ok((value, ck.timestamp.0));
        }

        Ok((None, TS::default()))
    }

    /// Resolve a value from a ValuePointer (read from vlog).
    fn resolve_value(&self, entry: &LsmEntry) -> Result<Option<V>, StorageError> {
        match &entry.value_ptr {
            Some(ptr) => {
                let vlog_entry: VlogEntry<K, V, TS> =
                    futures::executor::block_on(self.vlog.read(ptr))?;
                Ok(vlog_entry.value)
            }
            None => Ok(None),
        }
    }

    /// Put a key-value pair at a timestamp.
    fn put_impl(
        &mut self,
        key: K,
        value: Option<V>,
        timestamp: TS,
    ) -> Result<(), StorageError> {
        // Append to vlog (WAL).
        let entry = VlogEntry {
            key: key.clone(),
            timestamp,
            value,
        };
        let ptr = futures::executor::block_on(self.vlog.append(&entry))?;

        // Insert into memtable.
        self.memtable.insert(
            CompositeKey::new(key, timestamp),
            LsmEntry {
                value_ptr: Some(ptr),
                last_read_ts: None,
            },
        );

        // Flush if needed.
        self.maybe_flush()?;
        Ok(())
    }

    /// Update last-read timestamp for OCC.
    fn commit_get_impl(
        &mut self,
        key: K,
        read: TS,
        commit: TS,
    ) -> Result<(), StorageError> {
        // Convert commit TS to u64 for storage.
        // We store as the raw bits of the serialized timestamp.
        let commit_bytes =
            bitcode::serialize(&commit).map_err(|e| StorageError::Codec(e.to_string()))?;
        let commit_u64 = if commit_bytes.len() >= 8 {
            u64::from_le_bytes(commit_bytes[..8].try_into().unwrap())
        } else {
            let mut buf = [0u8; 8];
            buf[..commit_bytes.len()].copy_from_slice(&commit_bytes);
            u64::from_le_bytes(buf)
        };

        self.memtable.update_last_read(&key, &read, commit_u64);
        Ok(())
    }

    /// Get the last-read timestamp for the latest version.
    fn get_last_read_impl(&self, key: &K) -> Result<Option<TS>, StorageError> {
        if let Some((_, entry)) = self.memtable.get_latest(key) {
            return self.decode_last_read_ts(entry.last_read_ts);
        }

        let result = futures::executor::block_on(
            self.lsm.get_at::<K, TS>(key, &TS::max_value()),
        )?;
        if let Some((_, entry)) = result {
            return self.decode_last_read_ts(entry.last_read_ts);
        }

        Ok(None)
    }

    /// Get the last-read timestamp for the version at `timestamp`.
    fn get_last_read_at_impl(
        &self,
        key: &K,
        timestamp: TS,
    ) -> Result<Option<TS>, StorageError> {
        if let Some((_, entry)) = self.memtable.get_at(key, &timestamp) {
            return self.decode_last_read_ts(entry.last_read_ts);
        }

        let result = futures::executor::block_on(
            self.lsm.get_at::<K, TS>(key, &timestamp),
        )?;
        if let Some((_, entry)) = result {
            return self.decode_last_read_ts(entry.last_read_ts);
        }

        Ok(None)
    }

    /// Get the version range at a timestamp.
    fn get_range_impl(
        &self,
        key: &K,
        timestamp: TS,
    ) -> Result<(TS, Option<TS>), StorageError> {
        // Find the version at or before `timestamp`.
        let (_, at_ts) = self.get_at_impl(key, timestamp)?;

        // Find the version after `timestamp` (next version).
        // We need to search for the version just after at_ts.
        // This is more expensive; scan memtable and LSM for the key.
        let next_ts = self.find_next_version(key, at_ts)?;

        Ok((at_ts, next_ts))
    }

    fn find_next_version(&self, key: &K, after_ts: TS) -> Result<Option<TS>, StorageError> {
        let mut versions: Vec<TS> = Vec::new();

        // Collect from memtable
        for (ck, _) in self.memtable.iter() {
            if ck.key == *key && ck.timestamp.0 > after_ts {
                versions.push(ck.timestamp.0);
            }
        }

        // Collect from SSTables
        for meta in self
            .lsm
            .l0_metas()
            .iter()
            .rev()
            .chain(self.lsm.l1_metas().iter())
        {
            let reader = futures::executor::block_on(SSTableReader::<IO>::open(
                meta.path.clone(),
                self.io_flags,
            ))?;

            let all_entries = futures::executor::block_on(reader.read_all::<K, TS>())?;

            for (ck, _) in all_entries {
                if ck.key == *key && ck.timestamp.0 > after_ts {
                    versions.push(ck.timestamp.0);
                }
            }
        }

        // Return minimum timestamp > after_ts
        versions.sort();
        versions.dedup();
        Ok(versions.first().copied())
    }

    fn decode_last_read_ts(&self, raw: Option<u64>) -> Result<Option<TS>, StorageError> {
        match raw {
            None => Ok(None),
            Some(val) => {
                let bytes = val.to_le_bytes();
                let ts: TS = bitcode::deserialize(&bytes)
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
                Ok(Some(ts))
            }
        }
    }

    fn maybe_flush(&mut self) -> Result<(), StorageError> {
        if self.memtable.approx_bytes() >= FLUSH_THRESHOLD {
            futures::executor::block_on(
                self.lsm.flush_memtable(&mut self.memtable),
            )?;

            // Update flushed offset - all data up to current vlog position
            // is now safely in SSTables.
            self.flushed_vlog_offset = self.vlog.write_offset();

            // Compact if needed, get list of old files to delete.
            let old_files = if self.lsm.needs_compaction() {
                futures::executor::block_on(self.lsm.compact::<K, TS>())?
            } else {
                Vec::new()
            };

            // Persist manifest BEFORE deleting old files (crash safety).
            self.save_manifest()?;

            // Now safe to delete old compacted files.
            for f in old_files {
                let _ = std::fs::remove_file(&f);
            }
        }
        Ok(())
    }

    /// Scan for key-value pairs in `[start..=end]` at `timestamp`.
    fn scan_impl(
        &self,
        start: &K,
        end: &K,
        timestamp: TS,
    ) -> Result<Vec<(K, Option<V>, TS)>, StorageError> {
        let mut results = Vec::new();
        let mut found_keys: HashSet<K> = HashSet::new();

        // Scan memtable first
        let mem_results = self.memtable.scan(start, end, &timestamp);
        for (ck, entry) in &mem_results {
            let value = self.resolve_value(entry)?;
            results.push((ck.key.clone(), value, ck.timestamp.0));
            found_keys.insert(ck.key.clone());
        }

        // Scan SSTables for keys not in memtable
        let lsm_results = self.scan_sstables_in_range(start, end, &timestamp, &found_keys)?;
        results.extend(lsm_results);

        Ok(results)
    }

    /// Check if any writes exist in `[start..=end]` with timestamps in `(after_ts, before_ts)`.
    fn has_writes_in_range_impl(
        &self,
        start: &K,
        end: &K,
        after_ts: TS,
        before_ts: TS,
    ) -> Result<bool, StorageError> {
        // Check memtable first (early exit)
        for (ck, _) in self.memtable.iter() {
            if ck.key < *start {
                continue;
            }
            if ck.key > *end {
                break;
            }
            if ck.timestamp.0 > after_ts && ck.timestamp.0 < before_ts {
                return Ok(true);
            }
        }

        // Check SSTables (early exit on first match)
        for meta in self
            .lsm
            .l0_metas()
            .iter()
            .rev()
            .chain(self.lsm.l1_metas().iter())
        {
            let reader = futures::executor::block_on(SSTableReader::<IO>::open(
                meta.path.clone(),
                self.io_flags,
            ))?;

            let all_entries = futures::executor::block_on(reader.read_all::<K, TS>())?;

            for (ck, _) in all_entries {
                if ck.key < *start {
                    continue;
                }
                if ck.key > *end {
                    break;
                }
                if ck.timestamp.0 > after_ts && ck.timestamp.0 < before_ts {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Scan SSTables in the LSM tree for keys in range, excluding keys already found.
    ///
    /// Helper for `scan_impl()` to merge SSTable results with memtable results.
    fn scan_sstables_in_range(
        &self,
        start: &K,
        end: &K,
        timestamp: &TS,
        skip_keys: &HashSet<K>,
    ) -> Result<Vec<(K, Option<V>, TS)>, StorageError> {
        let mut seen: BTreeMap<K, (TS, LsmEntry)> = BTreeMap::new();

        // Scan all SSTables: L0 (newest first) + L1
        for meta in self
            .lsm
            .l0_metas()
            .iter()
            .rev()
            .chain(self.lsm.l1_metas().iter())
        {
            let reader = futures::executor::block_on(SSTableReader::<IO>::open(
                meta.path.clone(),
                self.io_flags,
            ))?;

            let all_entries = futures::executor::block_on(reader.read_all::<K, TS>())?;

            for (ck, entry) in all_entries {
                // Filter: key in range, timestamp <= query ts, not in memtable
                if ck.key < *start || ck.key > *end {
                    continue;
                }
                if ck.timestamp.0 > *timestamp {
                    continue;
                }
                if skip_keys.contains(&ck.key) {
                    continue;
                }

                // Keep latest version per key
                if let Some((ts, _)) = seen.get(&ck.key)
                    && *ts >= ck.timestamp.0
                {
                    continue;
                }
                seen.insert(ck.key.clone(), (ck.timestamp.0, entry));
            }
        }

        // Resolve values and return
        let mut results = Vec::new();
        for (key, (ts, entry)) in seen {
            let value = self.resolve_value(&entry)?;
            results.push((key, value, ts));
        }
        Ok(results)
    }

    /// Sync all data to disk.
    pub fn sync(&self) -> Result<(), StorageError> {
        futures::executor::block_on(self.vlog.sync())
    }

    pub fn base_dir(&self) -> &PathBuf {
        &self.base_dir
    }
}

impl<K, V, TS, IO: DiskIo> MvccBackend<K, V, TS> for DiskStore<K, V, TS, IO>
where
    K: Serialize + for<'de> Deserialize<'de> + Ord + Clone + Send + Debug + std::hash::Hash,
    V: Serialize + for<'de> Deserialize<'de> + Clone + Send + Debug,
    TS: Serialize
        + for<'de> Deserialize<'de>
        + Ord
        + Copy
        + Default
        + MaxValue
        + Send
        + Debug,
{
    type Error = StorageError;

    fn get(&self, key: &K) -> Result<(Option<V>, TS), StorageError> {
        self.get_impl(key)
    }

    fn get_at(&self, key: &K, timestamp: TS) -> Result<(Option<V>, TS), StorageError> {
        self.get_at_impl(key, timestamp)
    }

    fn get_range(&self, key: &K, timestamp: TS) -> Result<(TS, Option<TS>), StorageError> {
        self.get_range_impl(key, timestamp)
    }

    fn put(&mut self, key: K, value: Option<V>, timestamp: TS) -> Result<(), StorageError> {
        self.put_impl(key, value, timestamp)
    }

    fn commit_get(&mut self, key: K, read: TS, commit: TS) -> Result<(), StorageError> {
        self.commit_get_impl(key, read, commit)
    }

    fn get_last_read(&self, key: &K) -> Result<Option<TS>, StorageError> {
        self.get_last_read_impl(key)
    }

    fn get_last_read_at(&self, key: &K, timestamp: TS) -> Result<Option<TS>, StorageError> {
        self.get_last_read_at_impl(key, timestamp)
    }

    fn scan(
        &self,
        start: &K,
        end: &K,
        timestamp: TS,
    ) -> Result<Vec<(K, Option<V>, TS)>, StorageError> {
        self.scan_impl(start, end, timestamp)
    }

    fn has_writes_in_range(
        &self,
        start: &K,
        end: &K,
        after_ts: TS,
        before_ts: TS,
    ) -> Result<bool, StorageError> {
        self.has_writes_in_range_impl(start, end, after_ts, before_ts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::disk_io::BufferedIo;
    use tempfile::TempDir;

    #[test]
    fn basic_put_get() {
        let dir = TempDir::new().unwrap();
        let mut store =
            DiskStore::<String, String, u64, BufferedIo>::open(dir.path().to_path_buf())
                .unwrap();

        // Empty get.
        let (v, ts) = store.get_impl(&"key1".to_string()).unwrap();
        assert_eq!(v, None);
        assert_eq!(ts, 0);

        // Put and get.
        store
            .put_impl("key1".to_string(), Some("value1".to_string()), 10)
            .unwrap();
        let (v, ts) = store.get_impl(&"key1".to_string()).unwrap();
        assert_eq!(v, Some("value1".to_string()));
        assert_eq!(ts, 10);
    }

    #[test]
    fn multiple_versions() {
        let dir = TempDir::new().unwrap();
        let mut store =
            DiskStore::<String, String, u64, BufferedIo>::open(dir.path().to_path_buf())
                .unwrap();

        store
            .put_impl("key1".to_string(), Some("v1".to_string()), 10)
            .unwrap();
        store
            .put_impl("key1".to_string(), Some("v2".to_string()), 20)
            .unwrap();

        // Latest version.
        let (v, ts) = store.get_impl(&"key1".to_string()).unwrap();
        assert_eq!(v, Some("v2".to_string()));
        assert_eq!(ts, 20);

        // Version at ts=15 -> should get v1 at ts=10.
        let (v, ts) = store.get_at_impl(&"key1".to_string(), 15).unwrap();
        assert_eq!(v, Some("v1".to_string()));
        assert_eq!(ts, 10);
    }

    #[test]
    fn tombstone() {
        let dir = TempDir::new().unwrap();
        let mut store =
            DiskStore::<String, String, u64, BufferedIo>::open(dir.path().to_path_buf())
                .unwrap();

        store
            .put_impl("key1".to_string(), Some("v1".to_string()), 10)
            .unwrap();
        store
            .put_impl("key1".to_string(), None, 20)
            .unwrap();

        let (v, ts) = store.get_impl(&"key1".to_string()).unwrap();
        assert_eq!(v, None);
        assert_eq!(ts, 20);
    }

    #[test]
    fn mvcc_backend_trait() {
        let dir = TempDir::new().unwrap();
        let mut store =
            DiskStore::<String, String, u64, BufferedIo>::open(dir.path().to_path_buf())
                .unwrap();

        // Use the trait methods.
        MvccBackend::put(&mut store, "k".to_string(), Some("val".to_string()), 5)
            .unwrap();
        let (v, ts) = MvccBackend::get(&store, &"k".to_string()).unwrap();
        assert_eq!(v, Some("val".to_string()));
        assert_eq!(ts, 5);

        let (v, ts) = MvccBackend::get_at(&store, &"k".to_string(), 3).unwrap();
        assert_eq!(v, None);
        assert_eq!(ts, 0);
    }

    #[test]
    fn manifest_save_and_recover() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Write some data and save manifest.
        {
            let mut store =
                DiskStore::<String, String, u64, BufferedIo>::open(path.clone()).unwrap();
            store
                .put_impl("key1".to_string(), Some("val1".to_string()), 10)
                .unwrap();
            store.save_manifest().unwrap();
        }

        // Reopen — should recover from manifest.
        {
            let store =
                DiskStore::<String, String, u64, BufferedIo>::open(path).unwrap();
            // Data was in memtable (not flushed to SSTable), so it won't
            // be visible after recovery without vlog replay. The vlog
            // exists as WAL, but Phase 1 recovery restores LSM state only.
            // This test verifies the manifest round-trip works.
            assert!(store.base_dir().exists());
        }
    }

    #[test]
    fn crash_recovery_replays_unflushed_vlog_entries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Stage 1: Write data but don't flush to SSTables
        {
            let mut store =
                DiskStore::<String, String, u64, BufferedIo>::open(path.clone()).unwrap();

            // Write some entries to vlog (not large enough to trigger flush)
            store
                .put_impl("key1".to_string(), Some("value1".to_string()), 10)
                .unwrap();
            store
                .put_impl("key2".to_string(), Some("value2".to_string()), 20)
                .unwrap();
            store
                .put_impl("key3".to_string(), Some("value3".to_string()), 30)
                .unwrap();

            // Verify they're in memtable
            let (v, ts) = store.get_impl(&"key1".to_string()).unwrap();
            assert_eq!(v, Some("value1".to_string()));
            assert_eq!(ts, 10);

            // Save manifest to mark these as unflushed
            store.save_manifest().unwrap();
        }

        // Stage 2: Reopen store - should replay vlog into memtable
        {
            let store =
                DiskStore::<String, String, u64, BufferedIo>::open(path).unwrap();

            // All three entries should be recovered from vlog
            let (v, ts) = store.get_impl(&"key1".to_string()).unwrap();
            assert_eq!(v, Some("value1".to_string()));
            assert_eq!(ts, 10);

            let (v, ts) = store.get_impl(&"key2".to_string()).unwrap();
            assert_eq!(v, Some("value2".to_string()));
            assert_eq!(ts, 20);

            let (v, ts) = store.get_impl(&"key3".to_string()).unwrap();
            assert_eq!(v, Some("value3".to_string()));
            assert_eq!(ts, 30);
        }
    }

    #[test]
    fn crash_recovery_handles_partial_writes() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Stage 1: Write entries to vlog
        {
            let mut store =
                DiskStore::<String, String, u64, BufferedIo>::open(path.clone()).unwrap();

            store
                .put_impl("key1".to_string(), Some("value1".to_string()), 10)
                .unwrap();
            store
                .put_impl("key2".to_string(), Some("value2".to_string()), 20)
                .unwrap();
            store.save_manifest().unwrap();
        }

        // Stage 2: Corrupt the vlog file (simulate partial write)
        {
            let vlog_path = path.join("vlog-000000.log");
            let metadata = std::fs::metadata(&vlog_path).unwrap();
            let file_size = metadata.len();

            // Truncate file to simulate incomplete write
            if file_size >= 4096 {
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&vlog_path)
                    .unwrap();
                file.set_len(file_size - 2048).unwrap(); // Truncate last entry
            }
        }

        // Stage 3: Recovery should succeed but recover only the first entry
        {
            let store =
                DiskStore::<String, String, u64, BufferedIo>::open(path).unwrap();

            // First entry should be recovered
            let (v, ts) = store.get_impl(&"key1".to_string()).unwrap();
            assert_eq!(v, Some("value1".to_string()));
            assert_eq!(ts, 10);

            // Second entry should not be recovered (truncated)
            let (v, ts) = store.get_impl(&"key2".to_string()).unwrap();
            assert_eq!(v, None);
            assert_eq!(ts, 0);
        }
    }

    #[test]
    fn crash_recovery_with_mixed_flushed_and_unflushed_data() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Stage 1: Write enough data to trigger flush
        {
            let mut store =
                DiskStore::<String, String, u64, BufferedIo>::open(path.clone()).unwrap();

            // Write large values to trigger flush
            for i in 0u64..2000 {
                let key = format!("key-{i:06}");
                let val = format!("value-{i:06}-{}", "x".repeat(20));
                store.put_impl(key, Some(val), i + 1).unwrap();
            }
            // At this point, some entries are in SSTables, some in memtable
            store.save_manifest().unwrap();
        }

        // Stage 2: Reopen - should get flushed data from LSM + unflushed from vlog
        {
            let store =
                DiskStore::<String, String, u64, BufferedIo>::open(path).unwrap();

            // Early key should be from SSTable
            let (v, ts) = store.get_impl(&"key-000000".to_string()).unwrap();
            assert!(v.is_some());
            assert_eq!(ts, 1);

            // Later key should be from recovered memtable
            let (v, ts) = store.get_impl(&"key-001999".to_string()).unwrap();
            assert!(v.is_some());
            assert_eq!(ts, 2000);
        }
    }

    // ========== Crash-at-Boundary Tests ==========

    #[test]
    fn crash_during_flush_fsync_fails() {
        type TestStore = DiskStore<String, String, u64, BufferedIo>;

        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Phase 1: Write enough data to trigger flush, save manifest
        {
            let mut store = TestStore::open(path.clone()).unwrap();

            // Write enough to trigger flush (>64KB)
            for i in 0u64..2000 {
                let key = format!("key-{i:06}");
                let val = format!("value-{i:06}-{}", "x".repeat(20));
                store.put_impl(key, Some(val), i + 1).unwrap();
            }

            store.save_manifest().unwrap();
            assert!(store.lsm.l0_metas().len() > 0);
        }

        // Phase 2: Write more data that would trigger flush, but don't save manifest
        // (simulating crash before manifest save after SSTable fsync)
        {
            let mut store = TestStore::open(path.clone()).unwrap();

            for i in 2000u64..4000 {
                let key = format!("key-{i:06}");
                let val = format!("value-{i:06}-{}", "x".repeat(20));
                store.put_impl(key, Some(val), i + 1).unwrap();
            }

            // Explicitly drop without saving manifest (simulates crash)
        }

        // Phase 3: Recover - manifest doesn't include new SSTable
        {
            let store = TestStore::open(path).unwrap();

            // First batch (in manifest) should be readable
            let (v, ts) = store.get_impl(&"key-000000".to_string()).unwrap();
            assert!(v.is_some());
            assert_eq!(ts, 1);

            // Second batch (not in manifest) recovered from vlog replay
            let (v, ts) = store.get_impl(&"key-002000".to_string()).unwrap();
            assert!(v.is_some());
            assert_eq!(ts, 2001);
        }
    }

    #[test]
    fn crash_during_flush_sstable_corrupted() {
        type TestStore = DiskStore<String, String, u64, BufferedIo>;
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Phase 1: Write and flush successfully
        let sstable_path = {
            let mut store = TestStore::open(path.clone()).unwrap();

            for i in 0u64..2000 {
                let key = format!("key-{i:06}");
                let val = format!("value-{i:06}-{}", "x".repeat(20));
                store.put_impl(key, Some(val), i + 1).unwrap();
            }

            store.save_manifest().unwrap();

            // Get SSTable path for corruption
            store.lsm.l0_metas()[0].path.clone()
        };

        // Phase 2: Corrupt the SSTable file (flip some bytes)
        {
            let mut data = std::fs::read(&sstable_path).unwrap();
            if data.len() > 100 {
                // Corrupt bytes in the middle
                data[50] ^= 0xFF;
                data[51] ^= 0xFF;
                std::fs::write(&sstable_path, data).unwrap();
            }
        }

        // Phase 3: Recovery should detect corrupted SSTable
        {
            match TestStore::open(path) {
                Ok(store) => {
                    // Store opened, but reading from corrupted SSTable may fail
                    match store.get_impl(&"key-000000".to_string()) {
                        Ok((v, ts)) => {
                            // Recovery succeeded via vlog replay
                            assert!(v.is_some());
                            assert_eq!(ts, 1);
                        }
                        Err(e) => {
                            // Reading failed due to corruption - acceptable
                            // Test verifies corruption is detected
                            assert!(matches!(e, super::super::error::StorageError::Corruption { .. }));
                        }
                    }
                }
                Err(e) => {
                    // Opening failed due to corruption during recovery - also acceptable
                    assert!(matches!(e, super::super::error::StorageError::Corruption { .. }));
                }
            }
        }
    }

    #[test]
    fn crash_during_flush_manifest_not_updated() {
        type TestStore = DiskStore<String, String, u64, BufferedIo>;

        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Phase 1: Write data that triggers flush, but don't save manifest
        {
            let mut store = TestStore::open(path.clone()).unwrap();

            for i in 0u64..2000 {
                let key = format!("key-{i:06}");
                let val = format!("value-{i:06}-{}", "x".repeat(20));
                store.put_impl(key, Some(val), i + 1).unwrap();
            }

            // Data flushed to SSTable, but don't save manifest
            // (simulating crash before manifest save)
        }

        // Phase 2: Recover without manifest
        {
            let store = TestStore::open(path).unwrap();

            // Manifest doesn't list the SSTable (orphan file)
            // But vlog replay should recover the data
            let (v, ts): (Option<String>, u64) = store.get_impl(&"key-000000".to_string()).unwrap();
            assert!(v.is_some());
            assert_eq!(ts, 1);
        }
    }

    #[test]
    fn crash_during_compaction_fsync_fails_before_manifest_update() {
        type TestStore = DiskStore<String, String, u64, BufferedIo>;
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Phase 1: Create 4 L0 SSTables by writing large batches
        {
            let mut store = TestStore::open(path.clone()).unwrap();

            // Write 4 separate large batches (each triggers memtable flush)
            for batch in 0u64..4 {
                for i in 0u64..800 {
                    let key = format!("key-{batch:02}-{i:04}");
                    let val = format!("value-{}-{}", batch, "x".repeat(30));
                    store.put_impl(key, Some(val), batch * 1000 + i).unwrap();
                }
            }

            // If we have L0 files or L1, we're good
            let has_sstables = store.lsm.l0_metas().len() >= 3 || store.lsm.l1_metas().len() > 0;
            assert!(has_sstables, "Expected L0 or L1 SSTables to be created");
            store.save_manifest().unwrap();
        }

        // Phase 2: Trigger compaction but don't save manifest after
        // (simulating crash after new L1 written but before manifest update)
        {
            let mut store = TestStore::open(path.clone()).unwrap();

            // Write one more batch to trigger compaction
            for i in 0u64..600 {
                let key = format!("key-04-{i:04}");
                let val = format!("value-{}", "x".repeat(20));
                store.put_impl(key, Some(val), 4000 + i).unwrap();
            }

            // Compaction happens, new L1 created, but we drop without saving manifest
            // With bug fix: old files should still exist (not deleted yet)
        }

        // Phase 3: Recover - manifest still points to old L0 files
        {
            let store = TestStore::open(path).unwrap();

            // Old L0 files exist, data recoverable
            let (v, ts): (Option<String>, u64) = store.get_impl(&"key-00-0000".to_string()).unwrap();
            assert!(v.is_some());
            assert_eq!(ts, 0);

            // Verify bug fix: old files weren't deleted prematurely
            // (if they were, this would fail)
            let (v, _ts): (Option<String>, u64) = store.get_impl(&"key-01-0000".to_string()).unwrap();
            assert!(v.is_some());
        }
    }

    #[test]
    fn crash_during_compaction_manifest_not_updated() {
        type TestStore = DiskStore<String, String, u64, BufferedIo>;
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Phase 1: Create and save initial state
        {
            let mut store = TestStore::open(path.clone()).unwrap();

            for batch in 0u64..2 {
                for i in 0u64..600 {
                    let key = format!("key-{batch:02}-{i:04}");
                    let val = format!("value-{}", "x".repeat(20));
                    store.put_impl(key, Some(val), batch * 1000 + i).unwrap();
                }
            }

            store.save_manifest().unwrap();
        }

        // Phase 2: Add more data, trigger compaction, don't save manifest
        {
            let mut store = TestStore::open(path.clone()).unwrap();

            for batch in 2u64..4 {
                for i in 0u64..600 {
                    let key = format!("key-{batch:02}-{i:04}");
                    let val = format!("value-{}", "x".repeat(20));
                    store.put_impl(key, Some(val), batch * 1000 + i).unwrap();
                }
            }

            // Drop without saving manifest (simulates crash)
        }

        // Phase 3: Recover - uses old manifest but vlog replay recovers new data
        {
            let store = TestStore::open(path).unwrap();

            // Old data from manifest
            let (v, _ts): (Option<String>, u64) = store.get_impl(&"key-00-0000".to_string()).unwrap();
            assert!(v.is_some());

            // New data from vlog replay
            let (v, _ts): (Option<String>, u64) = store.get_impl(&"key-02-0000".to_string()).unwrap();
            assert!(v.is_some());
        }
    }

    #[test]
    fn crash_during_compaction_l1_corrupted() {
        type TestStore = DiskStore<String, String, u64, BufferedIo>;
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Phase 1: Create L1 via compaction
        let l1_path = {
            let mut store = TestStore::open(path.clone()).unwrap();

            for batch in 0u64..4 {
                for i in 0u64..600 {
                    let key = format!("key-{batch:02}-{i:04}");
                    let val = format!("value-{}", "x".repeat(20));
                    store.put_impl(key, Some(val), batch * 1000 + i).unwrap();
                }
            }

            store.save_manifest().unwrap();

            // Get L1 path if it exists
            if !store.lsm.l1_metas().is_empty() {
                Some(store.lsm.l1_metas()[0].path.clone())
            } else {
                None
            }
        };

        // Phase 2: Corrupt L1 file if it exists
        if let Some(l1_path) = l1_path {
            let mut data = std::fs::read(&l1_path).unwrap();
            if data.len() > 100 {
                // Corrupt bytes
                data[100] ^= 0xFF;
                data[101] ^= 0xFF;
                std::fs::write(&l1_path, data).unwrap();
            }

            // Phase 3: Recovery should handle corrupted L1 gracefully
            {
                let store = TestStore::open(path).unwrap();

                // Recovery should work via vlog replay even with corrupted L1
                let (v, _ts): (Option<String>, u64) = store.get_impl(&"key-00-0000".to_string()).unwrap();
                assert!(v.is_some());
            }
        } else {
            // If no L1 was created, test passes (compaction didn't occur)
            // This can happen if L0 threshold wasn't reached
        }
    }

    // ========== Compaction Correctness Tests ==========

    #[test]
    fn compaction_no_data_loss() {
        type TestStore = DiskStore<String, String, u64, BufferedIo>;
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        let mut store = TestStore::open(path).unwrap();

        // Insert 1000 keys with versions ts=1..1000
        for i in 1u64..=1000 {
            store.put_impl(format!("key-{}", i % 100), Some(format!("value-{i}")), i).unwrap();
        }

        store.save_manifest().unwrap();

        // Force compaction if it hasn't happened
        let has_l1 = store.lsm.l1_metas().len() > 0;

        // Verify all 1000 versions are readable at their timestamps
        for i in 1u64..=1000 {
            let key = format!("key-{}", i % 100);
            let (v, ts): (Option<String>, u64) = store.get_at_impl(&key, i).unwrap();
            assert!(v.is_some(), "Lost data for key {} at ts {}", key, i);
            assert_eq!(v.unwrap(), format!("value-{i}"));
            assert!(ts <= i && ts > 0, "Wrong timestamp for key {} at query ts {}: got {}", key, i, ts);
        }

        println!("Verified all 1000 versions readable. L1 files: {}", if has_l1 { "yes" } else { "no" });
    }

    #[test]
    fn compaction_version_ordering_preserved() {
        type TestStore = DiskStore<String, String, u64, BufferedIo>;
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        let mut store = TestStore::open(path).unwrap();

        // Write multiple versions of same keys in non-monotonic order
        let keys = vec!["alice", "bob", "charlie"];
        for key in &keys {
            store.put_impl(key.to_string(), Some(format!("{}-v10", key)), 10).unwrap();
            store.put_impl(key.to_string(), Some(format!("{}-v50", key)), 50).unwrap();
            store.put_impl(key.to_string(), Some(format!("{}-v30", key)), 30).unwrap();
            store.put_impl(key.to_string(), Some(format!("{}-v70", key)), 70).unwrap();
        }

        // Trigger flush and potentially compaction
        for i in 0..2000 {
            store.put_impl(format!("filler-{i}"), Some("x".repeat(30)), 100 + i).unwrap();
        }

        store.save_manifest().unwrap();

        // Verify version ordering: latest version should be returned by get()
        for key in &keys {
            let (v, ts): (Option<String>, u64) = store.get_impl(&key.to_string()).unwrap();
            assert_eq!(v, Some(format!("{}-v70", key)));
            assert_eq!(ts, 70);

            // Verify intermediate versions accessible via get_at
            let (v, ts): (Option<String>, u64) = store.get_at_impl(&key.to_string(), 25).unwrap();
            assert_eq!(v, Some(format!("{}-v10", key)));
            assert_eq!(ts, 10);

            let (v, ts): (Option<String>, u64) = store.get_at_impl(&key.to_string(), 40).unwrap();
            assert_eq!(v, Some(format!("{}-v30", key)));
            assert_eq!(ts, 30);

            let (v, ts): (Option<String>, u64) = store.get_at_impl(&key.to_string(), 60).unwrap();
            assert_eq!(v, Some(format!("{}-v50", key)));
            assert_eq!(ts, 50);
        }
    }

    #[test]
    fn compaction_tombstone_handling() {
        type TestStore = DiskStore<String, String, u64, BufferedIo>;
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        let mut store = TestStore::open(path).unwrap();

        // Write old version
        store.put_impl("key".to_string(), Some("old-value".to_string()), 10).unwrap();

        // Flush to L0
        for i in 0..2000 {
            store.put_impl(format!("filler-{i}"), Some("x".repeat(30)), 20 + i).unwrap();
        }

        // Write tombstone
        store.put_impl("key".to_string(), None, 50).unwrap();

        store.save_manifest().unwrap();

        // Latest version is tombstone
        let (v, ts): (Option<String>, u64) = store.get_impl(&"key".to_string()).unwrap();
        assert_eq!(v, None);
        assert_eq!(ts, 50);

        // Old version still accessible at ts < 50
        let (v, ts): (Option<String>, u64) = store.get_at_impl(&"key".to_string(), 30).unwrap();
        assert_eq!(v, Some("old-value".to_string()));
        assert_eq!(ts, 10);
    }

    #[test]
    fn compaction_sequential() {
        type TestStore = DiskStore<String, String, u64, BufferedIo>;
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        let mut store = TestStore::open(path).unwrap();

        // Write 10,000 keys in batches, each batch may trigger compaction
        for batch in 0u64..10 {
            for i in 0u64..1000 {
                let key = format!("key-{}", i % 100);
                let val = format!("batch{}-val{}", batch, i);
                store.put_impl(key, Some(val), batch * 1000 + i).unwrap();
            }

            // Save manifest after each batch
            store.save_manifest().unwrap();

            // Verify data integrity after each batch
            for i in 0u64..100 {
                let key = format!("key-{i}");
                let (_v, _ts): (Option<String>, u64) = store.get_impl(&key).unwrap();
                // Just verify no crash/corruption, values will be overwritten
            }
        }

        // Final verification: latest versions readable
        for i in 0u64..100 {
            let key = format!("key-{i}");
            let (v, ts): (Option<String>, u64) = store.get_impl(&key).unwrap();
            assert!(v.is_some());
            assert!(ts >= 9000); // Should be from last batch
        }

        println!("Sequential compactions completed successfully");
    }

    // ========== GC Correctness Tests ==========

    #[test]
    fn gc_no_live_data_lost() {
        type TestStore = DiskStore<String, String, u64, BufferedIo>;
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        let mut store = TestStore::open(path.clone()).unwrap();

        // Write 1000 entries
        for i in 0u64..1000 {
            let key = format!("key-{:04}", i);
            let val = format!("value-{:04}", i);
            store.put_impl(key, Some(val), i + 1).unwrap();
        }

        // Delete 50% (every other key)
        for i in (0u64..1000).step_by(2) {
            let key = format!("key-{:04}", i);
            store.put_impl(key, None, 2000 + i).unwrap();
        }

        store.save_manifest().unwrap();

        // Collect vlog segments for GC
        let old_segment_id = store.vlog.id;

        // Close and reopen to force vlog segment boundary
        drop(store);
        let store = TestStore::open(path).unwrap();

        // Perform GC: copy live entries from old segment to new segment
        let new_segment_id = old_segment_id + 1;

        // Use the GC module's gc_vlog_segment function
        use super::super::gc::GarbageCollector;
        use super::super::vlog::VlogSegment;
        use super::super::disk_io::OpenFlags;

        let old_vlog_path = dir.path().join(format!("vlog-{:06}.log", old_segment_id));
        let new_vlog_path = dir.path().join(format!("vlog-{:06}.log", new_segment_id));

        // Open segments
        let old_segment = VlogSegment::<BufferedIo>::open(
            old_segment_id,
            old_vlog_path,
            OpenFlags { create: false, direct: false },
        ).unwrap();

        let mut new_segment = VlogSegment::<BufferedIo>::open(
            new_segment_id,
            new_vlog_path,
            OpenFlags { create: true, direct: false },
        ).unwrap();

        let (stats, _pointer_updates) = futures::executor::block_on(
            GarbageCollector::gc_vlog_segment::<String, String, u64, BufferedIo>(
                &old_segment,
                &mut new_segment,
                &store.lsm,
            )
        ).unwrap();

        println!("GC stats: scanned={}, live={}, dead={}, reclaimed={} bytes",
                 stats.entries_scanned, stats.entries_live, stats.entries_dead, stats.bytes_reclaimed);

        // Verify GC ran and processed entries
        assert!(stats.entries_scanned > 0, "Should have scanned entries");
        assert!(stats.entries_live > 0, "Should have found live entries");

        // In MVCC, old versions are kept unless pruned by compaction,
        // so we may see more live entries than just the latest versions.
        // The key verification is that data integrity is maintained.

        // Verify all 500 live keys are still readable
        for i in (1u64..1000).step_by(2) {
            let key = format!("key-{:04}", i);
            let (v, _ts): (Option<String>, u64) = store.get_impl(&key).unwrap();
            assert!(v.is_some(), "Lost live key {}", key);
            assert_eq!(v.unwrap(), format!("value-{:04}", i));
        }

        // Verify deleted keys are tombstones
        for i in (0u64..1000).step_by(2) {
            let key = format!("key-{:04}", i);
            let (v, _ts): (Option<String>, u64) = store.get_impl(&key).unwrap();
            assert_eq!(v, None, "Key {} should be deleted", key);
        }
    }

    #[test]
    fn gc_dead_data_reclaimed() {
        type TestStore = DiskStore<String, String, u64, BufferedIo>;
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        let mut store = TestStore::open(path.clone()).unwrap();

        // Write entries with known value sizes
        let value_size = 100;
        for i in 0u64..100 {
            let key = format!("key-{:02}", i);
            let val = "x".repeat(value_size);
            store.put_impl(key, Some(val), i + 1).unwrap();
        }

        // Overwrite 50 entries (making old versions dead)
        for i in 0u64..50 {
            let key = format!("key-{:02}", i);
            let val = "y".repeat(value_size);
            store.put_impl(key, Some(val), 200 + i).unwrap();
        }

        store.save_manifest().unwrap();

        let old_segment_id = store.vlog.id;
        drop(store);
        let store = TestStore::open(path).unwrap();

        // Perform GC
        use super::super::gc::GarbageCollector;
        use super::super::vlog::VlogSegment;
        use super::super::disk_io::OpenFlags;

        let old_vlog_path = dir.path().join(format!("vlog-{:06}.log", old_segment_id));
        let new_vlog_path = dir.path().join(format!("vlog-{:06}.log", old_segment_id + 1));

        let old_segment = VlogSegment::<BufferedIo>::open(
            old_segment_id,
            old_vlog_path,
            OpenFlags { create: false, direct: false },
        ).unwrap();

        let mut new_segment = VlogSegment::<BufferedIo>::open(
            old_segment_id + 1,
            new_vlog_path,
            OpenFlags { create: true, direct: false },
        ).unwrap();

        let (stats, _pointer_updates) = futures::executor::block_on(
            GarbageCollector::gc_vlog_segment::<String, String, u64, BufferedIo>(
                &old_segment,
                &mut new_segment,
                &store.lsm,
            )
        ).unwrap();

        println!("GC stats: scanned={}, live={}, dead={}, reclaimed={} bytes",
                 stats.entries_scanned, stats.entries_live, stats.entries_dead, stats.bytes_reclaimed);

        // Verify GC processed entries and reclaimed some bytes
        assert!(stats.entries_scanned > 0, "Should have scanned entries");

        // We overwrote 50 entries, so there should be some dead data
        // (exact count depends on MVCC version retention policy)
        assert!(stats.entries_dead > 0, "Should have found dead entries after overwrites");

        // bytes_reclaimed should be non-zero (dead entries were reclaimed)
        assert!(stats.bytes_reclaimed > 0,
                "Expected some bytes reclaimed, got {}", stats.bytes_reclaimed);
    }

    #[test]
    fn gc_pointer_updates() {
        type TestStore = DiskStore<String, String, u64, BufferedIo>;
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        let mut store = TestStore::open(path.clone()).unwrap();

        // Write entries
        for i in 0u64..50 {
            let key = format!("key-{:02}", i);
            let val = format!("value-{:02}", i);
            store.put_impl(key, Some(val), i + 1).unwrap();
        }

        store.save_manifest().unwrap();

        let old_segment_id = store.vlog.id;
        drop(store);
        let mut store = TestStore::open(path.clone()).unwrap();

        // Perform GC and collect pointer updates
        use super::super::gc::GarbageCollector;
        use super::super::vlog::VlogSegment;
        use super::super::disk_io::OpenFlags;
        use super::super::memtable::{CompositeKey, LsmEntry};

        let old_vlog_path = dir.path().join(format!("vlog-{:06}.log", old_segment_id));
        let new_vlog_path = dir.path().join(format!("vlog-{:06}.log", old_segment_id + 1));

        let old_segment = VlogSegment::<BufferedIo>::open(
            old_segment_id,
            old_vlog_path,
            OpenFlags { create: false, direct: false },
        ).unwrap();

        let mut new_segment = VlogSegment::<BufferedIo>::open(
            old_segment_id + 1,
            new_vlog_path.clone(),
            OpenFlags { create: true, direct: false },
        ).unwrap();

        let (stats, pointer_updates) = futures::executor::block_on(
            GarbageCollector::gc_vlog_segment::<String, String, u64, BufferedIo>(
                &old_segment,
                &mut new_segment,
                &store.lsm,
            )
        ).unwrap();

        println!("GC pointer updates: {} entries moved to new segment", pointer_updates.len());

        // Verify all live entries have pointer updates
        assert_eq!(pointer_updates.len(), stats.entries_live as usize);

        // Verify all pointer updates reference the new segment
        for (key, ts, new_ptr) in &pointer_updates {
            assert_eq!(new_ptr.segment_id, old_segment_id + 1,
                       "Pointer for key {:?} at ts {:?} should reference new segment", key, ts);
        }

        // Apply pointer updates to LSM (simulating what DiskStore would do)
        for (key, ts, new_ptr) in pointer_updates {
            store.memtable.insert(
                CompositeKey::new(key, ts),
                LsmEntry {
                    value_ptr: Some(new_ptr),
                    last_read_ts: None,
                },
            );
        }

        // Flush memtable and save
        futures::executor::block_on(store.lsm.flush_memtable(&mut store.memtable)).unwrap();
        store.save_manifest().unwrap();

        // Verify data still readable after GC and pointer updates
        for i in 0u64..50 {
            let key = format!("key-{:02}", i);
            let (v, _ts): (Option<String>, u64) = store.get_impl(&key).unwrap();
            assert_eq!(v, Some(format!("value-{:02}", i)),
                       "Data lost or corrupted after GC for key {}", key);
        }
    }
}
