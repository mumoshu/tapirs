use super::disk_io::{DiskIo, OpenFlags};
use super::error::StorageError;
use super::memtable::{CompositeKey, LsmEntry, MaxValue, Memtable};
use super::sstable::{SSTableReader, SSTableWriter};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Maximum number of L0 SSTables before triggering compaction.
const L0_MAX_FILES: usize = 4;

/// SSTable metadata stored in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableMeta {
    pub id: u64,
    pub level: u32,
    pub path: PathBuf,
    pub num_entries: u64,
}

/// 2-level LSM tree managing SSTables on disk.
pub struct LsmTree<IO: DiskIo> {
    base_dir: PathBuf,
    /// L0: unsorted SSTables (direct memtable flushes).
    l0: Vec<SSTableMeta>,
    /// L1: sorted, non-overlapping SSTables.
    l1: Vec<SSTableMeta>,
    /// Next SSTable ID.
    next_sst_id: u64,
    /// I/O flags for opening SSTables.
    io_flags: OpenFlags,
    _io: std::marker::PhantomData<IO>,
}

impl<IO: DiskIo> LsmTree<IO> {
    pub fn new(base_dir: PathBuf, io_flags: OpenFlags) -> Self {
        Self {
            base_dir,
            l0: Vec::new(),
            l1: Vec::new(),
            next_sst_id: 0,
            io_flags,
            _io: std::marker::PhantomData,
        }
    }

    /// Restore from manifest data.
    pub fn restore(
        base_dir: PathBuf,
        l0: Vec<SSTableMeta>,
        l1: Vec<SSTableMeta>,
        next_sst_id: u64,
        io_flags: OpenFlags,
    ) -> Self {
        Self {
            base_dir,
            l0,
            l1,
            next_sst_id,
            io_flags,
            _io: std::marker::PhantomData,
        }
    }

    pub fn l0_metas(&self) -> &[SSTableMeta] {
        &self.l0
    }

    pub fn l1_metas(&self) -> &[SSTableMeta] {
        &self.l1
    }

    pub fn next_sst_id(&self) -> u64 {
        self.next_sst_id
    }

    /// Flush a memtable to a new L0 SSTable.
    pub async fn flush_memtable<K, TS>(
        &mut self,
        memtable: &mut Memtable<K, TS>,
    ) -> Result<SSTableMeta, StorageError>
    where
        K: Serialize + Clone + Ord,
        TS: Serialize + Clone + Ord,
    {
        let entries = memtable.drain();
        if entries.is_empty() {
            return Err(StorageError::Codec("empty memtable".into()));
        }

        let sst_id = self.next_sst_id;
        self.next_sst_id += 1;
        let path = self.sst_path(sst_id);

        let num_entries = SSTableWriter::write::<K, TS, IO>(&path, &entries, self.io_flags).await?;

        let meta = SSTableMeta {
            id: sst_id,
            level: 0,
            path,
            num_entries,
        };
        self.l0.push(meta.clone());
        Ok(meta)
    }

    /// Check if L0 compaction is needed.
    pub fn needs_compaction(&self) -> bool {
        self.l0.len() >= L0_MAX_FILES
    }

    /// Compact all L0 SSTables + L1 into new L1 SSTables.
    ///
    /// Returns a list of old SSTable files to be deleted by the caller
    /// AFTER the manifest has been persisted. This ensures crash safety:
    /// if we crash before manifest save, old files still exist and recovery works.
    pub async fn compact<K, TS>(&mut self) -> Result<Vec<PathBuf>, StorageError>
    where
        K: Serialize + for<'de> Deserialize<'de> + Ord + Clone,
        TS: Serialize + for<'de> Deserialize<'de> + Ord + Clone,
    {
        if self.l0.is_empty() {
            return Ok(Vec::new());
        }

        // Read all entries from L0 and L1.
        let mut merged = BTreeMap::new();

        // L1 entries first (older).
        for meta in &self.l1 {
            let reader = SSTableReader::<IO>::open(meta.path.clone(), self.io_flags).await?;
            for (ck, entry) in reader.read_all::<K, TS>().await? {
                merged.insert(ck, entry);
            }
        }

        // L0 entries override L1 (newer). Process oldest L0 first.
        for meta in &self.l0 {
            let reader = SSTableReader::<IO>::open(meta.path.clone(), self.io_flags).await?;
            for (ck, entry) in reader.read_all::<K, TS>().await? {
                merged.insert(ck, entry);
            }
        }

        if merged.is_empty() {
            return Ok(Vec::new());
        }

        // Write merged result as new L1 SSTable(s).
        let sst_id = self.next_sst_id;
        self.next_sst_id += 1;
        let path = self.sst_path(sst_id);
        let num_entries = SSTableWriter::write::<K, TS, IO>(&path, &merged, self.io_flags).await?;

        let new_meta = SSTableMeta {
            id: sst_id,
            level: 1,
            path,
            num_entries,
        };

        // Build list of old files to be deleted by caller AFTER manifest save.
        let old_files: Vec<PathBuf> = self
            .l0
            .iter()
            .chain(self.l1.iter())
            .map(|m| m.path.clone())
            .collect();

        self.l0.clear();
        self.l1 = vec![new_meta];
        Ok(old_files)
    }

    /// Look up the latest version of `key` with timestamp <= `ts`.
    ///
    /// Search order: L0 (newest first) -> L1.
    pub async fn get_at<K, TS>(
        &self,
        key: &K,
        ts: &TS,
    ) -> Result<Option<(CompositeKey<K, TS>, LsmEntry)>, StorageError>
    where
        K: Serialize + for<'de> Deserialize<'de> + Ord + Clone,
        TS: Serialize + for<'de> Deserialize<'de> + Ord + Clone + MaxValue,
    {
        // Search L0 (newest first).
        for meta in self.l0.iter().rev() {
            let reader = SSTableReader::<IO>::open(meta.path.clone(), self.io_flags).await?;
            if !reader.may_contain_key(key).await? {
                continue;
            }
            if let Some(result) = reader.get_at(key, ts).await? {
                return Ok(Some(result));
            }
        }

        // Search L1.
        for meta in &self.l1 {
            let reader = SSTableReader::<IO>::open(meta.path.clone(), self.io_flags).await?;
            if !reader.may_contain_key(key).await? {
                continue;
            }
            if let Some(result) = reader.get_at(key, ts).await? {
                return Ok(Some(result));
            }
        }

        Ok(None)
    }

    /// Get all SSTable file paths (for manifest).
    pub fn all_sst_paths(&self) -> Vec<PathBuf> {
        self.l0
            .iter()
            .chain(self.l1.iter())
            .map(|m| m.path.clone())
            .collect()
    }

    fn sst_path(&self, id: u64) -> PathBuf {
        self.base_dir.join(format!("sst-{id:06}.db"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::disk_io::BufferedIo;
    use super::super::vlog::ValuePointer;
    use tempfile::TempDir;

    fn test_flags() -> OpenFlags {
        OpenFlags {
            create: true,
            direct: false,
        }
    }

    #[tokio::test]
    async fn flush_and_lookup() {
        let dir = TempDir::new().unwrap();
        let mut lsm = LsmTree::<BufferedIo>::new(dir.path().to_path_buf(), test_flags());

        let mut mt = Memtable::<String, u64>::new();
        for i in 0u64..5 {
            mt.insert(
                CompositeKey::new(format!("key-{i:03}"), i * 10),
                LsmEntry {
                    value_ptr: Some(ValuePointer {
                        segment_id: 1,
                        offset: i * 100,
                        length: 50,
                    }),
                    last_read_ts: None,
                },
            );
        }

        lsm.flush_memtable(&mut mt).await.unwrap();
        assert!(mt.is_empty());
        assert_eq!(lsm.l0.len(), 1);

        // Look up.
        let result = lsm
            .get_at::<String, u64>(&"key-003".to_string(), &100)
            .await
            .unwrap();
        assert!(result.is_some());
        let (ck, entry) = result.unwrap();
        assert_eq!(ck.timestamp.0, 30);
        assert_eq!(entry.value_ptr.unwrap().offset, 300);
    }

    #[tokio::test]
    async fn compaction() {
        let dir = TempDir::new().unwrap();
        let mut lsm = LsmTree::<BufferedIo>::new(dir.path().to_path_buf(), test_flags());

        // Flush 4 memtables to trigger compaction threshold.
        for batch in 0u64..4 {
            let mut mt = Memtable::<u64, u64>::new();
            for i in 0u64..3 {
                mt.insert(
                    CompositeKey::new(i, batch * 10 + i),
                    LsmEntry {
                        value_ptr: None,
                        last_read_ts: None,
                    },
                );
            }
            lsm.flush_memtable(&mut mt).await.unwrap();
        }

        assert_eq!(lsm.l0.len(), 4);
        assert!(lsm.needs_compaction());

        let old_files = lsm.compact::<u64, u64>().await.unwrap();
        assert_eq!(old_files.len(), 4); // 4 L0 files to delete
        assert_eq!(lsm.l0.len(), 0);
        assert_eq!(lsm.l1.len(), 1);

        // Simulate what disk_store does: delete old files after compaction
        for f in old_files {
            let _ = std::fs::remove_file(&f);
        }

        // Verify data is still accessible.
        let result = lsm.get_at::<u64, u64>(&1, &100).await.unwrap();
        assert!(result.is_some());
    }
}
