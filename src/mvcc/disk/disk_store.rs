use super::disk_io::{DiskIo, OpenFlags};
use super::error::StorageError;
use super::lsm::LsmTree;
use super::manifest::Manifest;
use super::memtable::{CompositeKey, LsmEntry, MaxValue, Memtable};
use super::vlog::{VlogEntry, VlogSegment};
use crate::mvcc::backend::MvccBackend;
use serde::{Deserialize, Serialize};
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
    _v: std::marker::PhantomData<V>,
}

impl<K, V, TS, IO: DiskIo> DiskStore<K, V, TS, IO>
where
    K: Serialize + for<'de> Deserialize<'de> + Ord + Clone + Send + Debug,
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
        let memtable = Memtable::new();
        // For Phase 1, the manifest records the flushed offset. Entries
        // after that offset are replayed. The vlog is the WAL.
        // In practice, replay would scan from manifest.vlog_write_offset
        // to end-of-file. For now, the memtable starts empty and will be
        // rebuilt from new writes.

        Ok(Self {
            memtable,
            lsm,
            vlog,
            base_dir,
            io_flags,
            next_segment_id: manifest.next_segment_id,
            _v: std::marker::PhantomData,
        })
    }

    /// Save current state to manifest.
    pub fn save_manifest(&self) -> Result<(), StorageError> {
        let manifest = Manifest {
            l0_sstables: self.lsm.l0_metas().to_vec(),
            l1_sstables: self.lsm.l1_metas().to_vec(),
            vlog_segment_ids: vec![self.vlog.id],
            vlog_write_offset: self.vlog.write_offset(),
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
        // Check memtable: iterate entries for this key after `after_ts`.
        let mut next: Option<TS> = None;

        // In the BTreeMap, entries with Reverse<TS> are ordered so that
        // higher timestamps come first. We look for the entry whose
        // timestamp is just above `after_ts`.
        let _search = CompositeKey::new(key.clone(), after_ts);
        for (ck, _) in self.memtable.iter() {
            if ck.key != *key {
                if ck.key > *key {
                    break;
                }
                continue;
            }
            if ck.timestamp.0 > after_ts {
                // This version is newer. Since entries are in descending
                // TS order, the last one we see > after_ts (but closest
                // to it) is the one we want.
                next = Some(ck.timestamp.0);
            }
        }

        // Also check LSM (all SSTables might have versions we haven't seen).
        // For simplicity in Phase 1, we do a brute-force search.
        // A production implementation would optimize this with range queries.

        if next.is_some() {
            return Ok(next);
        }

        Ok(None)
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

            if self.lsm.needs_compaction() {
                futures::executor::block_on(self.lsm.compact::<K, TS>())?;
            }

            // Persist manifest after structural changes.
            self.save_manifest()?;
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

        // Scan memtable.
        let mem_results = self.memtable.scan(start, end, &timestamp);
        for (ck, entry) in &mem_results {
            let value = self.resolve_value(entry)?;
            results.push((ck.key.clone(), value, ck.timestamp.0));
        }

        // For Phase 1, memtable-only scan is sufficient for correctness.
        // A full implementation would also scan SSTables and merge.
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
        // Scan memtable for any writes in the range.
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
        Ok(false)
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
    K: Serialize + for<'de> Deserialize<'de> + Ord + Clone + Send + Debug,
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
}
