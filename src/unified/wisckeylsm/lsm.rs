use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use crate::unified::wisckeylsm::sst::{SstMeta, SSTableReader, SSTableWriter};
use crate::unified::wisckeylsm::types::{VlogPtr, VlogSegmentMeta};
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
    /// Construct a VlogLsm from pre-opened parts.
    pub(crate) fn open_from_parts(
        label: &str,
        base_dir: &Path,
        active_vlog: VlogSegment<IO>,
        sealed_segments: BTreeMap<u64, VlogSegment<IO>>,
        io_flags: OpenFlags,
        next_segment_id: u64,
        max_index_entries: usize,
    ) -> Self {
        Self {
            memtable: BTreeMap::new(),
            index: BTreeMap::new(),
            sst_readers: Vec::new(),
            sst_metas: Vec::new(),
            next_sst_id: 0,
            max_index_entries,
            active_vlog,
            sealed_segments,
            entry_count: 0,
            base_dir: base_dir.to_path_buf(),
            io_flags,
            next_segment_id,
            label: label.to_string(),
        }
    }

    /// Insert a key → VlogPtr mapping into the index (used during recovery).
    pub(crate) fn index_insert(&mut self, key: K, ptr: VlogPtr) {
        self.index.insert(key, ptr);
    }

    /// Access the in-memory K→VlogPtr index.
    pub(crate) fn index(&self) -> &BTreeMap<K, VlogPtr> {
        &self.index
    }

    /// Reference to the active vlog segment.
    pub(crate) fn active_vlog_ref(&self) -> &VlogSegment<IO> {
        &self.active_vlog
    }

    /// Reference to sealed vlog segments.
    pub(crate) fn sealed_segments_ref(&self) -> &BTreeMap<u64, VlogSegment<IO>> {
        &self.sealed_segments
    }

    /// Active vlog segment id.
    pub(crate) fn active_vlog_id(&self) -> u64 {
        self.active_vlog.id
    }

    /// Active vlog segment write offset.
    pub(crate) fn active_write_offset(&self) -> u64 {
        self.active_vlog.write_offset()
    }

    /// Next segment id (for manifest persistence).
    pub(crate) fn next_segment_id(&self) -> u64 {
        self.next_segment_id
    }

    /// Remove a key from both memtable and index. Returns the memtable value
    /// if it was present. Vlog data is not removed (managed by compaction).
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.index.remove(key);
        self.memtable.remove(key)
    }

    /// Insert a key-value pair into the memtable. No vlog/index write —
    /// those happen at `seal_view()` time via the caller-provided closure.
    pub fn put(&mut self, key: K, value: V) {
        self.memtable.insert(key, value);
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

    /// Flush memtable to vlog+index via `header_fn`, clear memtable, optionally
    /// flush index to SST, optionally rotate vlog. Returns sealed segment
    /// metadata if the vlog segment was rotated.
    ///
    /// `header_fn` is called for each memtable entry not already in the index.
    /// Return `Some((entry_type, client_id, number))` to flush to vlog, or
    /// `None` to discard.
    pub fn seal_view<F>(
        &mut self,
        min_vlog_size: u64,
        header_fn: F,
    ) -> Result<Option<VlogSegmentMeta>, StorageError>
    where
        K: Serialize + DeserializeOwned + Clone,
        V: Serialize,
        F: Fn(&K, &V) -> Option<(u8, u64, u64)>,
    {
        // Batch-flush memtable entries not already in the index.
        let mut batch_keys: Vec<K> = Vec::new();
        let mut batch_raw: Vec<(u8, u64, u64, Vec<u8>)> = Vec::new();
        for (key, value) in &self.memtable {
            if self.index.contains_key(key) {
                continue; // recovery entry — already in vlog
            }
            if let Some((entry_type, client_id, number)) = header_fn(key, value) {
                let payload = bitcode::serialize(value)
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
                batch_keys.push(key.clone());
                batch_raw.push((entry_type, client_id, number, payload));
            }
        }
        if !batch_raw.is_empty() {
            let raw_refs: Vec<(u8, u64, u64, &[u8])> = batch_raw
                .iter()
                .map(|(et, cid, num, bytes)| (*et, *cid, *num, bytes.as_slice()))
                .collect();
            let ptrs = self.active_vlog.append_raw_batch(&raw_refs)?;
            for (key, ptr) in batch_keys.into_iter().zip(ptrs) {
                self.index.insert(key, ptr);
            }
            self.entry_count += batch_raw.len() as u32;
        }

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
        let sealed_meta = if segment_size >= min_vlog_size {
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
                    sealed_path.clone(),
                    sealed_size,
                    sealed_views.clone(),
                    self.io_flags,
                )?,
            );
            old.close();

            Some(VlogSegmentMeta {
                segment_id: sealed_id,
                path: sealed_path,
                views: sealed_views,
                total_size: sealed_size,
            })
        } else {
            None
        };

        self.entry_count = 0;
        Ok(sealed_meta)
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

    pub(crate) fn segment_ref(&self, id: u64) -> Option<&VlogSegment<IO>> {
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

    fn open_test_lsm(
        dir: &Path,
        max_index_entries: usize,
    ) -> VlogLsm<String, String, MemoryIo> {
        MemoryIo::create_dir_all(dir).unwrap();
        let vlog_path = dir.join("vlog_seg_0000.dat");
        let mut active_vlog =
            VlogSegment::<MemoryIo>::open(0, vlog_path, test_flags()).unwrap();
        active_vlog.start_view(0);
        VlogLsm::open_from_parts(
            "",
            dir,
            active_vlog,
            BTreeMap::new(),
            test_flags(),
            1,
            max_index_entries,
        )
    }

    #[test]
    fn get_missing_key_returns_none() {
        let dir = MemoryIo::temp_path();
        let lsm = open_test_lsm(&dir, 4);
        assert_eq!(lsm.get(&"missing".to_string()).unwrap(), None);
    }

    #[test]
    fn put_and_get_from_memtable() {
        let dir = MemoryIo::temp_path();
        let mut lsm = open_test_lsm(&dir, 4);

        lsm.put("k_a".into(), "val_a".into());
        lsm.put("k_b".into(), "val_b".into());

        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));
        assert_eq!(lsm.get(&"missing".into()).unwrap(), None);
    }

    fn test_header_fn(_k: &String, _v: &String) -> Option<(u8, u64, u64)> {
        Some((0x80, 0, 0))
    }

    #[test]
    fn seal_reads_from_index_then_sst() {
        let dir = MemoryIo::temp_path();
        // max_index_entries=4 so index flushes to SST after accumulating 4 entries.
        let mut lsm = open_test_lsm(&dir, 4);

        // Put k_a, k_b (2 entries in index, below threshold).
        lsm.put("k_a".into(), "val_a".into());
        lsm.put("k_b".into(), "val_b".into());

        // Seal #1: memtable cleared, index stays (2 < 4).
        // min_vlog_size=0 forces vlog rotation.
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(1);

        // Reads go through index → vlog.
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));

        // Put k_c, k_d (index now has 4 entries: k_a, k_b, k_c, k_d).
        lsm.put("k_c".into(), "val_c".into());
        lsm.put("k_d".into(), "val_d".into());

        // k_c from memtable (current view).
        assert_eq!(lsm.get(&"k_c".into()).unwrap(), Some("val_c".into()));

        // Seal #2: index has 4 entries >= threshold → flushed to SST, cleared.
        lsm.seal_view(0, test_header_fn).unwrap();
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
        let mut lsm = open_test_lsm(&dir, 4);

        // (1) Missing key.
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), None);

        // (2) Put k_a, k_b → memtable reads.
        lsm.put("k_a".into(), "val_a".into());
        lsm.put("k_b".into(), "val_b".into());
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));

        // (3) Seal #1: index stays (2 < 4), vlog rotated.
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(1);
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));

        // Put k_c, k_d into new segment.
        lsm.put("k_c".into(), "val_c".into());
        lsm.put("k_d".into(), "val_d".into());
        assert_eq!(lsm.get(&"k_c".into()).unwrap(), Some("val_c".into()));

        // (4) Seal #2: index has 4 >= 4 → flushed to SST.
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(2);
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));
        assert_eq!(lsm.get(&"k_c".into()).unwrap(), Some("val_c".into()));
        assert_eq!(lsm.get(&"k_d".into()).unwrap(), Some("val_d".into()));

        // (5) Put k_e into seg 2, verify mixed reads.
        lsm.put("k_e".into(), "val_e".into());
        assert_eq!(lsm.get(&"k_e".into()).unwrap(), Some("val_e".into())); // memtable
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into())); // SST → vlog
        assert_eq!(lsm.get(&"k_c".into()).unwrap(), Some("val_c".into())); // SST → vlog

        // (6) Seal #3: index has 1 entry (k_e) < 4, stays in index.
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(3);
        assert_eq!(lsm.get(&"k_e".into()).unwrap(), Some("val_e".into())); // index → vlog
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into())); // SST → vlog
        assert_eq!(lsm.get(&"k_d".into()).unwrap(), Some("val_d".into())); // SST → vlog

        // (7) Missing key still returns None.
        assert_eq!(lsm.get(&"missing".into()).unwrap(), None);
    }
}
