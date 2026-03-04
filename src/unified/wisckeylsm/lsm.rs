use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use crate::unified::wisckeylsm::sst::{SstMeta, SSTableReader, SSTableWriter};
use crate::unified::wisckeylsm::types::VlogPtr;
use crate::unified::wisckeylsm::vlog::VlogSegment;

/// Generic WiscKey-style LSM that stores raw (K, V) pairs in a memtable for
/// the current view and maintains a K→VlogPtr index that accumulates across
/// seals. When the index exceeds a configurable threshold, `seal_view` flushes
/// it to a new SST and clears it.
///
/// Read path: memtable (fast clone) → index (VlogPtr → vlog) → SSTs (VlogPtr → vlog).
pub(crate) struct VlogLsm<K: Ord, V, IO: DiskIo> {
    /// Current view raw values, cleared on seal.
    memtable: BTreeMap<K, V>,
    /// All views K→VlogPtr, accumulates across seals, flushed to SST when large.
    index: BTreeMap<K, VlogPtr>,
    sst_readers: Vec<SSTableReader<IO>>,
    sst_metas: Vec<SstMeta>,
    next_sst_id: u64,
    max_index_entries: usize,
    active_vlog: VlogSegment<IO>,
    sealed_segments: BTreeMap<u64, VlogSegment<IO>>,
    entry_count: u32,
    base_dir: PathBuf,
    io_flags: OpenFlags,
    next_segment_id: u64,
    label: String,
}

impl<K: Ord, V, IO: DiskIo> VlogLsm<K, V, IO> {
    /// Create a fresh VlogLsm with an empty memtable, index, and a new vlog segment.
    pub fn open(
        label: &str,
        base_dir: &Path,
        io_flags: OpenFlags,
        max_index_entries: usize,
    ) -> Result<Self, StorageError> {
        IO::create_dir_all(base_dir)?;
        let vlog_path = Self::make_vlog_path(base_dir, label, 0);
        let mut active_vlog = VlogSegment::<IO>::open(0, vlog_path, io_flags)?;
        active_vlog.start_view(0);

        Ok(Self {
            memtable: BTreeMap::new(),
            index: BTreeMap::new(),
            sst_readers: Vec::new(),
            sst_metas: Vec::new(),
            next_sst_id: 0,
            max_index_entries,
            active_vlog,
            sealed_segments: BTreeMap::new(),
            entry_count: 0,
            base_dir: base_dir.to_path_buf(),
            io_flags,
            next_segment_id: 1,
            label: label.to_string(),
        })
    }

    /// Serialize V to the vlog, insert (K, V) into memtable, insert (K, VlogPtr)
    /// into the index. Atomic from the caller's perspective.
    pub fn put(
        &mut self,
        key: K,
        value: V,
        entry_type: u8,
        client_id: u64,
        number: u64,
    ) -> Result<(), StorageError>
    where
        K: Serialize + Clone,
        V: Serialize,
    {
        let payload =
            bitcode::serialize(&value).map_err(|e| StorageError::Codec(e.to_string()))?;
        let ptr =
            self.active_vlog
                .append_raw_entry(entry_type, client_id, number, &payload)?;
        self.entry_count += 1;
        self.index.insert(key.clone(), ptr);
        self.memtable.insert(key, value);
        Ok(())
    }

    /// Full read path: memtable (clone V) → index (VlogPtr → vlog) → SSTs
    /// (VlogPtr → vlog). Returns owned V.
    pub fn get(&self, key: &K) -> Result<Option<V>, StorageError>
    where
        K: Serialize + DeserializeOwned + Clone,
        V: Clone + DeserializeOwned,
    {
        if let Some(v) = self.memtable.get(key) {
            return Ok(Some(v.clone()));
        }

        if let Some(ptr) = self.index.get(key) {
            let v = self.read_value_from_vlog(ptr)?;
            return Ok(Some(v));
        }

        for reader in self.sst_readers.iter().rev() {
            if IO::block_on(reader.may_contain_key(key))?
                && let Some(ptr) = IO::block_on(reader.get::<K, VlogPtr>(key))?
            {
                let v = self.read_value_from_vlog(&ptr)?;
                return Ok(Some(v));
            }
        }

        Ok(None)
    }

    /// Clear memtable. If the index exceeds `max_index_entries`, flush it to a
    /// new SST and clear it. Rotate the vlog segment if it meets the size
    /// threshold.
    pub fn seal_view(&mut self, min_vlog_size: u64) -> Result<(), StorageError>
    where
        K: Serialize + DeserializeOwned + Clone,
    {
        self.active_vlog.sync()?;
        self.active_vlog.finish_view(self.entry_count);

        self.memtable.clear();

        if self.index.len() >= self.max_index_entries && !self.index.is_empty() {
            let id = self.next_sst_id;
            self.next_sst_id += 1;
            let path = self.sst_path(id);

            let n = IO::block_on(SSTableWriter::write::<K, VlogPtr, IO>(
                &path,
                &self.index,
                self.io_flags,
            ))?;
            let reader = IO::block_on(SSTableReader::open(path.clone(), self.io_flags))?;
            self.sst_readers.push(reader);
            self.sst_metas.push(SstMeta {
                id,
                path,
                num_entries: n,
            });
            self.index.clear();
        }

        let segment_size = self.active_vlog.write_offset();
        if segment_size >= min_vlog_size {
            let sealed_id = self.active_vlog.id;
            let sealed_path = self.active_vlog.path().clone();
            let sealed_views = self.active_vlog.views.clone();
            let sealed_size = self.active_vlog.write_offset();

            let new_id = self.next_segment_id;
            self.next_segment_id += 1;
            let new_path = self.vlog_path(new_id);
            let new_seg = VlogSegment::<IO>::open(new_id, new_path, self.io_flags)?;
            let old = std::mem::replace(&mut self.active_vlog, new_seg);

            self.sealed_segments.insert(
                sealed_id,
                VlogSegment::<IO>::open_at(
                    sealed_id,
                    sealed_path,
                    sealed_size,
                    sealed_views,
                    self.io_flags,
                )?,
            );
            old.close();
        }

        self.entry_count = 0;
        Ok(())
    }

    /// Start a new view on the active vlog segment.
    pub fn start_view(&mut self, view: u64) {
        self.active_vlog.start_view(view);
    }

    fn read_value_from_vlog(&self, ptr: &VlogPtr) -> Result<V, StorageError>
    where
        V: DeserializeOwned,
    {
        let segment = self.segment_ref(ptr.segment_id).ok_or_else(|| {
            StorageError::Codec(format!("VLog segment {} not found", ptr.segment_id))
        })?;
        let raw = segment.read_raw_entry(ptr)?;
        bitcode::deserialize(&raw.payload).map_err(|e| StorageError::Codec(e.to_string()))
    }

    fn segment_ref(&self, id: u64) -> Option<&VlogSegment<IO>> {
        if self.active_vlog.id == id {
            Some(&self.active_vlog)
        } else {
            self.sealed_segments.get(&id)
        }
    }

    fn make_vlog_path(base_dir: &Path, label: &str, id: u64) -> PathBuf {
        if label.is_empty() {
            base_dir.join(format!("vlog_seg_{id:04}.dat"))
        } else {
            base_dir.join(format!("{label}_vlog_{id:04}.dat"))
        }
    }

    fn vlog_path(&self, id: u64) -> PathBuf {
        Self::make_vlog_path(&self.base_dir, &self.label, id)
    }

    fn sst_path(&self, id: u64) -> PathBuf {
        if self.label.is_empty() {
            self.base_dir.join(format!("sst_{id:04}.db"))
        } else {
            self.base_dir.join(format!("{}_sst_{id:04}.db", self.label))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::disk::memory_io::MemoryIo;

    fn test_flags() -> OpenFlags {
        OpenFlags {
            create: true,
            direct: false,
        }
    }

    #[test]
    fn get_missing_key_returns_none() {
        let dir = MemoryIo::temp_path();
        let lsm =
            VlogLsm::<String, String, MemoryIo>::open("", &dir, test_flags(), 4).unwrap();
        assert_eq!(lsm.get(&"missing".to_string()).unwrap(), None);
    }

    #[test]
    fn put_and_get_from_memtable() {
        let dir = MemoryIo::temp_path();
        let mut lsm =
            VlogLsm::<String, String, MemoryIo>::open("", &dir, test_flags(), 4).unwrap();

        lsm.put("k_a".into(), "val_a".into(), 0x80, 1, 1).unwrap();
        lsm.put("k_b".into(), "val_b".into(), 0x80, 1, 2).unwrap();

        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));
        assert_eq!(lsm.get(&"missing".into()).unwrap(), None);
    }

    #[test]
    fn seal_reads_from_index_then_sst() {
        let dir = MemoryIo::temp_path();
        // max_index_entries=4 so index flushes to SST after accumulating 4 entries.
        let mut lsm =
            VlogLsm::<String, String, MemoryIo>::open("", &dir, test_flags(), 4).unwrap();

        // Put k_a, k_b (2 entries in index, below threshold).
        lsm.put("k_a".into(), "val_a".into(), 0x80, 1, 1).unwrap();
        lsm.put("k_b".into(), "val_b".into(), 0x80, 1, 2).unwrap();

        // Seal #1: memtable cleared, index stays (2 < 4).
        // min_vlog_size=0 forces vlog rotation.
        lsm.seal_view(0).unwrap();
        lsm.start_view(1);

        // Reads go through index → vlog.
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));

        // Put k_c, k_d (index now has 4 entries: k_a, k_b, k_c, k_d).
        lsm.put("k_c".into(), "val_c".into(), 0x80, 1, 3).unwrap();
        lsm.put("k_d".into(), "val_d".into(), 0x80, 1, 4).unwrap();

        // k_c from memtable (current view).
        assert_eq!(lsm.get(&"k_c".into()).unwrap(), Some("val_c".into()));

        // Seal #2: index has 4 entries >= threshold → flushed to SST, cleared.
        lsm.seal_view(0).unwrap();
        lsm.start_view(2);

        // All reads now go through SST → vlog.
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));
        assert_eq!(lsm.get(&"k_c".into()).unwrap(), Some("val_c".into()));
        assert_eq!(lsm.get(&"k_d".into()).unwrap(), Some("val_d".into()));
    }

    #[test]
    fn multi_seal_mixed_read_sources() {
        let dir = MemoryIo::temp_path();
        let mut lsm =
            VlogLsm::<String, String, MemoryIo>::open("", &dir, test_flags(), 4).unwrap();

        // (1) Missing key.
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), None);

        // (2) Put k_a, k_b → memtable reads.
        lsm.put("k_a".into(), "val_a".into(), 0x80, 1, 1).unwrap();
        lsm.put("k_b".into(), "val_b".into(), 0x80, 1, 2).unwrap();
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));

        // (3) Seal #1: index stays (2 < 4), vlog rotated.
        lsm.seal_view(0).unwrap();
        lsm.start_view(1);
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));

        // Put k_c, k_d into new segment.
        lsm.put("k_c".into(), "val_c".into(), 0x80, 1, 3).unwrap();
        lsm.put("k_d".into(), "val_d".into(), 0x80, 1, 4).unwrap();
        assert_eq!(lsm.get(&"k_c".into()).unwrap(), Some("val_c".into()));

        // (4) Seal #2: index has 4 >= 4 → flushed to SST.
        lsm.seal_view(0).unwrap();
        lsm.start_view(2);
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));
        assert_eq!(lsm.get(&"k_c".into()).unwrap(), Some("val_c".into()));
        assert_eq!(lsm.get(&"k_d".into()).unwrap(), Some("val_d".into()));

        // (5) Put k_e into seg 2, verify mixed reads.
        lsm.put("k_e".into(), "val_e".into(), 0x80, 1, 5).unwrap();
        assert_eq!(lsm.get(&"k_e".into()).unwrap(), Some("val_e".into())); // memtable
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into())); // SST → vlog
        assert_eq!(lsm.get(&"k_c".into()).unwrap(), Some("val_c".into())); // SST → vlog

        // (6) Seal #3: index has 1 entry (k_e) < 4, stays in index.
        lsm.seal_view(0).unwrap();
        lsm.start_view(3);
        assert_eq!(lsm.get(&"k_e".into()).unwrap(), Some("val_e".into())); // index → vlog
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into())); // SST → vlog
        assert_eq!(lsm.get(&"k_d".into()).unwrap(), Some("val_d".into())); // SST → vlog

        // (7) Missing key still returns None.
        assert_eq!(lsm.get(&"missing".into()).unwrap(), None);
    }
}
