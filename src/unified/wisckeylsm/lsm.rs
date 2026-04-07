use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use crate::unified::wisckeylsm::manifest::LsmManifestData;
use crate::unified::wisckeylsm::sst::{SstMeta, SSTableReader, SSTableWriter};
use crate::unified::wisckeylsm::types::{VlogPtr, VlogSegmentMeta};
use crate::mvcc::disk::aligned_buf::AlignedBuf;
use crate::unified::wisckeylsm::vlog::{RawVlogEntry, VlogSegment};

/// Abstraction over the in-memory buffer used by VlogLsm for the current view.
///
/// `K` is the key type, `V` is the value type returned by reads.
/// `Source` is the type accepted by `mem_insert` — for BTreeMap this equals `V`,
/// but custom implementations can accept a different type and convert internally.
pub(crate) trait Memtable<K, V> {
    type Source;
    fn mem_insert(&mut self, key: K, source: Self::Source);
    fn mem_get(&self, key: &K) -> Option<&V>;
    fn mem_iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a V)> where K: 'a, V: 'a;
    fn mem_range_from<'a>(&'a self, from: &K) -> impl Iterator<Item = (&'a K, &'a V)> where K: 'a, V: 'a;
    fn mem_len(&self) -> usize;
    fn mem_clear(&mut self);
}

impl<K: Ord, V> Memtable<K, V> for BTreeMap<K, V> {
    type Source = V;
    fn mem_insert(&mut self, key: K, value: V) {
        self.insert(key, value);
    }
    fn mem_get(&self, key: &K) -> Option<&V> {
        self.get(key)
    }
    fn mem_len(&self) -> usize {
        self.len()
    }
    fn mem_iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a V)> where K: 'a, V: 'a {
        self.iter()
    }
    fn mem_range_from<'a>(&'a self, from: &K) -> impl Iterator<Item = (&'a K, &'a V)> where K: 'a, V: 'a {
        self.range(from..)
    }
    fn mem_clear(&mut self) {
        self.clear();
    }
}

/// Controls whether VlogLsm maintains an in-memory K→VlogPtr index.
#[derive(Debug, Clone, Copy)]
pub(crate) enum IndexMode {
    /// Maintain full in-memory BTreeMap<K, VlogPtr> (current behavior).
    /// SSTs are only used for durability; reads go through the index.
    InMemory,
    /// No in-memory index; reads go directly to SSTs.
    SstOnly,
}

/// Generic WiscKey-style LSM that stores raw (K, V) pairs in a memtable for
/// the current view and maintains a K→VlogPtr index that accumulates across
/// seals. Each `seal_view` writes new entries to both vlog and a per-seal SST.
/// The index is never cleared — it is the complete in-memory map.
///
/// Read path: memtable (fast clone) → index (VlogPtr → vlog). SSTs are only
/// used to rebuild the index on open.
pub(crate) struct VlogLsm<K: Ord, V, IO: DiskIo, M: Memtable<K, V> = BTreeMap<K, V>> {
    /// Current-view raw values. Written by `put()`, read first by `get()`.
    /// Cleared on `seal_view()` after flushing to `active_vlog` + `index`.
    /// Keys here may also appear in `index` (recovery case — index wins for
    /// already-persisted entries; memtable wins for reads in `get()`).
    memtable: M,

    /// V is used in method signatures but not in struct fields (only via M's
    /// Memtable<K, V> bound). PhantomData<fn() -> V> marks V as used without
    /// implying ownership.
    _phantom_v: PhantomData<fn() -> V>,

    /// Accumulated K→VlogPtr across all sealed views. Each VlogPtr references
    /// a (segment_id, offset) in either `active_vlog` or `sealed_segments`.
    /// Grows on every `seal_view()`. Flushed to a new SST at each seal but
    /// never cleared — serves as the complete in-memory map.
    /// `None` in `SstOnly` mode — reads go directly to SSTs.
    index: Option<BTreeMap<K, VlogPtr>>,

    /// On-disk SST files containing flushed snapshots of `index`. Searched in
    /// reverse order (newest first) by `get()` after memtable and index miss.
    /// Each reader's K→VlogPtr entries resolve to `active_vlog` or
    /// `sealed_segments`. Paired 1:1 with `sst_metas` by position.
    sst_readers: Vec<SSTableReader<IO>>,

    /// Metadata (id, path, entry_count) for each SST, paired 1:1 with
    /// `sst_readers`. Used for manifest persistence; not read at query time.
    sst_metas: Vec<SstMeta>,

    /// Monotonic counter for SST file IDs. Incremented each time `seal_view()`
    /// flushes `index` to a new SST. Drives filename: `{label}_sst_{id:04}.db`.
    next_sst_id: u64,

    /// Current writable vlog segment. `seal_view()` appends serialized memtable
    /// entries here via `append_raw_batch()`, producing VlogPtrs stored in
    /// `index`. When its size exceeds `min_vlog_size` at seal time, it is moved
    /// to `sealed_segments` and replaced with a fresh empty segment.
    active_vlog: VlogSegment<IO>,

    /// Read-only vlog segments keyed by segment_id. Populated when `active_vlog`
    /// is rotated. VlogPtrs from `index` and `sst_readers` whose segment_id
    /// differs from `active_vlog.id` resolve here.
    sealed_segments: BTreeMap<u64, VlogSegment<IO>>,

    /// Number of entries appended to `active_vlog` in the current view.
    /// Passed to `active_vlog.finish_view()` at seal time, then reset to 0.
    entry_count: u32,

    /// Root directory for all vlog and SST files.
    base_dir: PathBuf,

    /// I/O flags (create, direct_io) propagated to all VlogSegment and SST opens.
    io_flags: OpenFlags,

    /// Monotonic counter for vlog segment IDs. Incremented when `active_vlog`
    /// rotates. Drives filename: `{label}_vlog_seg_{id:04}.dat`.
    next_segment_id: u64,

    /// Filename prefix distinguishing this LSM instance. Examples: `""` (TAPIR
    /// committed → `vlog_seg_XXXX.dat`), `"prep"` (TAPIR prepared →
    /// `prep_vlog_seg_XXXX.dat`), `"ir"` (IR record → `ir_vlog_XXXX.dat`).
    label: String,
}

impl<K: Ord, V, IO: DiskIo, M: Memtable<K, V>> VlogLsm<K, V, IO, M> {
    /// Construct a VlogLsm from pre-opened parts.
    ///
    /// Opens each SST from `sst_metas`, loads all (K, VlogPtr) entries into the
    /// in-memory index, and keeps readers for future SST path management.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_from_parts(
        label: &str,
        base_dir: &Path,
        active_vlog: VlogSegment<IO>,
        sealed_segments: BTreeMap<u64, VlogSegment<IO>>,
        io_flags: OpenFlags,
        next_segment_id: u64,
        sst_metas: Vec<SstMeta>,
        next_sst_id: u64,
        index_mode: IndexMode,
    ) -> Result<Self, StorageError>
    where
        K: Serialize + DeserializeOwned + Clone,
        M: Default,
    {
        let mut sst_readers = Vec::with_capacity(sst_metas.len());
        let mut index = match index_mode {
            IndexMode::InMemory => Some(BTreeMap::new()),
            IndexMode::SstOnly => None,
        };
        for meta in &sst_metas {
            let reader = IO::block_on(SSTableReader::open(meta.path.clone(), io_flags))?;
            if let Some(ref mut idx) = index {
                let entries: Vec<(K, VlogPtr)> = IO::block_on(reader.read_all())?;
                for (k, ptr) in entries {
                    idx.insert(k, ptr);
                }
            }
            sst_readers.push(reader);
        }
        Ok(Self {
            memtable: M::default(),
            _phantom_v: PhantomData,
            index,
            sst_readers,
            sst_metas,
            next_sst_id,
            active_vlog,
            sealed_segments,
            entry_count: 0,
            base_dir: base_dir.to_path_buf(),
            io_flags,
            next_segment_id,
            label: label.to_string(),
        })
    }

    /// Open a VlogLsm from persisted manifest data.
    ///
    /// Opens all sealed vlog segments, opens the active segment at the
    /// label-derived path, starts the current view, then delegates to
    /// `open_from_parts` for SST index loading.
    pub(crate) fn open_from_manifest(
        label: &str,
        base_dir: &Path,
        manifest_data: &LsmManifestData,
        current_view: u64,
        io_flags: OpenFlags,
        index_mode: IndexMode,
    ) -> Result<Self, StorageError>
    where
        K: Serialize + DeserializeOwned + Clone,
        M: Default,
    {
        let mut sealed_segments = BTreeMap::new();
        for seg_meta in &manifest_data.sealed_vlog_segments {
            let seg = VlogSegment::<IO>::open_at(
                seg_meta.segment_id,
                seg_meta.path.clone(),
                seg_meta.total_size,
                seg_meta.views.clone(),
                io_flags,
            )?;
            sealed_segments.insert(seg_meta.segment_id, seg);
        }

        let active_path =
            Self::make_vlog_path(base_dir, label, manifest_data.active_segment_id);
        let mut active_vlog = VlogSegment::<IO>::open_at(
            manifest_data.active_segment_id,
            active_path,
            manifest_data.active_write_offset,
            Vec::new(),
            io_flags,
        )?;
        active_vlog.start_view(current_view);

        Self::open_from_parts(
            label,
            base_dir,
            active_vlog,
            sealed_segments,
            io_flags,
            manifest_data.next_segment_id,
            manifest_data.sst_metas.clone(),
            manifest_data.next_sst_id,
            index_mode,
        )
    }

    /// Insert a key → VlogPtr mapping into the index (used during recovery).
    /// Panics in `SstOnly` mode — recovery callers must use `InMemory`.
    pub(crate) fn index_insert(&mut self, key: K, ptr: VlogPtr) {
        self.index
            .as_mut()
            .expect("index_insert requires IndexMode::InMemory")
            .insert(key, ptr);
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

    /// SST metadata for manifest persistence.
    pub(crate) fn sst_metas(&self) -> &[SstMeta] {
        &self.sst_metas
    }

    /// Next SST id (for manifest persistence).
    pub(crate) fn next_sst_id(&self) -> u64 {
        self.next_sst_id
    }

    /// Insert a key-value pair into the memtable. No vlog/index write —
    /// those happen at `seal_view()` time via the caller-provided closure.
    pub fn put(&mut self, key: K, source: M::Source) {
        self.memtable.mem_insert(key, source);
    }

    /// Full read path: memtable (clone V) → index (VlogPtr → vlog) → SSTs
    /// (VlogPtr → vlog). Returns owned V.
    pub fn get(&self, key: &K) -> Result<Option<V>, StorageError>
    where
        K: Serialize + DeserializeOwned + Clone,
        V: Clone + DeserializeOwned,
    {
        if let Some(v) = self.memtable.mem_get(key) {
            return Ok(Some(v.clone()));
        }

        if let Some(ref idx) = self.index
            && let Some(ptr) = idx.get(key)
        {
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

    /// Range lookup: return the first entry with key >= `from`.
    ///
    /// In `InMemory` mode, checks memtable then index and picks the smaller key.
    /// In `SstOnly` mode, checks memtable then searches all SSTs for the
    /// smallest key >= `from`.
    pub fn range_get_first(&self, from: &K) -> Result<Option<(K, V)>, StorageError>
    where
        K: Serialize + DeserializeOwned + Clone,
        V: Clone + DeserializeOwned,
    {
        let mem_entry = self.memtable.mem_range_from(from).next();

        // Persistent-store candidate: index (InMemory) or SSTs (SstOnly).
        let store_candidate: Option<(K, VlogPtr)> = match &self.index {
            Some(idx) => idx.range(from..).next().map(|(k, p)| (k.clone(), *p)),
            None => self.sst_range_get_first(from)?,
        };

        match (mem_entry, store_candidate) {
            (Some((mk, mv)), Some((ik, ip))) => {
                if *mk <= ik {
                    Ok(Some((mk.clone(), mv.clone())))
                } else {
                    Ok(Some((ik, self.read_value_from_vlog(&ip)?)))
                }
            }
            (Some((mk, mv)), None) => Ok(Some((mk.clone(), mv.clone()))),
            (None, Some((ik, ip))) => Ok(Some((ik, self.read_value_from_vlog(&ip)?))),
            (None, None) => Ok(None),
        }
    }

    /// Search all SSTs for the smallest (K, VlogPtr) with key >= `from`.
    /// Each SST contains only its seal's new entries, so we must check all.
    fn sst_range_get_first(&self, from: &K) -> Result<Option<(K, VlogPtr)>, StorageError>
    where
        K: Serialize + DeserializeOwned + Clone,
    {
        let mut best: Option<(K, VlogPtr)> = None;
        for reader in &self.sst_readers {
            if let Some((k, ptr)) =
                IO::block_on(reader.range_get_first::<K, VlogPtr>(from))?
            {
                match &best {
                    None => best = Some((k, ptr)),
                    Some((bk, _)) if k < *bk => best = Some((k, ptr)),
                    _ => {}
                }
            }
        }
        Ok(best)
    }

    /// Range accessor over the memtable (from `from` key onward).
    pub(crate) fn memtable_range_from(&self, from: &K) -> impl Iterator<Item = (&K, &V)> {
        self.memtable.mem_range_from(from)
    }

    /// Range accessor over the index.
    /// Panics in `SstOnly` mode — callers must use `InMemory`.
    pub(crate) fn index_range<R: std::ops::RangeBounds<K>>(
        &self,
        range: R,
    ) -> std::collections::btree_map::Range<'_, K, VlogPtr> {
        self.index
            .as_ref()
            .expect("index_range() requires IndexMode::InMemory")
            .range(range)
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
        // Batch-flush memtable entries to vlog.
        let mut batch_keys: Vec<K> = Vec::new();
        let mut batch_raw: Vec<(u8, u64, u64, Vec<u8>)> = Vec::new();
        for (key, value) in self.memtable.mem_iter() {
            if let Some((entry_type, client_id, number)) = header_fn(key, value) {
                let payload = bitcode::serialize(value)
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
                batch_keys.push(key.clone());
                batch_raw.push((entry_type, client_id, number, payload));
            }
        }
        let mut new_entries: BTreeMap<K, VlogPtr> = BTreeMap::new();
        if !batch_raw.is_empty() {
            let raw_refs: Vec<(u8, u64, u64, &[u8])> = batch_raw
                .iter()
                .map(|(et, cid, num, bytes)| (*et, *cid, *num, bytes.as_slice()))
                .collect();
            let ptrs = self.active_vlog.append_raw_batch(&raw_refs)?;
            for (key, ptr) in batch_keys.into_iter().zip(ptrs) {
                if let Some(ref mut idx) = self.index {
                    idx.insert(key.clone(), ptr);
                }
                new_entries.insert(key, ptr);
            }
            self.entry_count += batch_raw.len() as u32;
        }

        self.active_vlog.sync()?;
        self.active_vlog.finish_view(self.entry_count);

        self.memtable.mem_clear();

        self.write_sst(&new_entries)?;

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

    fn write_sst(&mut self, entries: &BTreeMap<K, VlogPtr>) -> Result<(), StorageError>
    where
        K: Serialize + DeserializeOwned + Clone,
    {
        if entries.is_empty() {
            return Ok(());
        }
        let id = self.next_sst_id;
        self.next_sst_id += 1;
        let path = self.sst_path(id);
        let n = IO::block_on(SSTableWriter::write::<K, VlogPtr, IO>(
            &path, entries, self.io_flags,
        ))?;
        let reader = IO::block_on(SSTableReader::open(path.clone(), self.io_flags))?;
        self.sst_readers.push(reader);
        self.sst_metas.push(SstMeta { id, path, num_entries: n });
        Ok(())
    }

    /// Start a new view on the active vlog segment.
    pub fn start_view(&mut self, view: u64) {
        self.active_vlog.start_view(view);
    }

    pub(crate) fn read_value_from_vlog(&self, ptr: &VlogPtr) -> Result<V, StorageError>
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

    /// Number of entries in the memtable.
    pub(crate) fn memtable_len(&self) -> usize {
        self.memtable.mem_len()
    }

    /// Read raw bytes from all sealed + active vlog segments.
    pub(crate) fn export_segment_bytes(&self) -> Result<Vec<Vec<u8>>, StorageError> {
        let mut result = Vec::new();
        for seg in self.sealed_segments.values() {
            let bytes = seg.read_all_bytes()?;
            if !bytes.is_empty() {
                result.push(bytes);
            }
        }
        let active_bytes = self.active_vlog.read_all_bytes()?;
        if !active_bytes.is_empty() {
            result.push(active_bytes);
        }
        Ok(result)
    }

    /// Serialize current memtable entries into vlog binary format.
    ///
    /// `header_fn` returns `Some((entry_type, client_id, number))` to include
    /// an entry, or `None` to skip it. Same interface as `seal_view`.
    pub(crate) fn encode_memtable_as_segment<F>(
        &self,
        header_fn: F,
    ) -> Result<Vec<u8>, StorageError>
    where
        K: Serialize,
        V: Serialize,
        F: Fn(&K, &V) -> Option<(u8, u64, u64)>,
    {
        let mut all_bytes = Vec::new();
        for (key, value) in self.memtable.mem_iter() {
            if let Some((entry_type, client_id, number)) = header_fn(key, value) {
                let payload = bitcode::serialize(value)
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
                let raw = VlogSegment::<IO>::encode_raw_entry(
                    entry_type, client_id, number, &payload,
                );
                all_bytes.extend_from_slice(&raw);
            }
        }
        Ok(all_bytes)
    }

    /// Write raw segment bytes as a new sealed vlog file and rebuild index entries.
    ///
    /// `key_fn` reconstructs `K` from a raw vlog entry's header fields.
    /// Returns `None` to skip entries (e.g., wrong entry type).
    /// Persist raw vlog entry bytes as a new sealed segment file.
    ///
    /// Used for sealed segments received via StartView payloads during
    /// view change. Creates a new sealed segment file on disk, adds
    /// entries to the in-memory index, and returns metadata for manifest
    /// registration.
    ///
    /// Does NOT touch the memtable or active segment. Those are for:
    /// - Live write path: `put()` → memtable → `seal_view()` → active segment
    /// - Delta appendum: `append_to_active()` → active segment
    ///
    /// Callers MUST push the returned `VlogSegmentMeta` into the manifest's
    /// `sealed_vlog_segments` so `sync_to_remote` uploads the segment to S3.
    pub(crate) fn persist_sealed_segment<F>(
        &mut self,
        bytes: &[u8],
        key_fn: F,
    ) -> Result<Option<VlogSegmentMeta>, StorageError>
    where
        K: Clone,
        F: Fn(&RawVlogEntry) -> Option<K>,
    {
        if bytes.is_empty() {
            return Ok(None);
        }
        let id = self.next_segment_id;
        self.next_segment_id += 1;
        let path = self.vlog_path(id);
        // Write raw bytes to a new segment file
        let io = IO::open(&path, self.io_flags, crate::mvcc::disk::disk_io::OpenMode::CreateNew)?;
        let mut buf = AlignedBuf::new(bytes.len());
        buf.as_full_slice_mut()[..bytes.len()].copy_from_slice(bytes);
        buf.set_len(bytes.len());
        IO::block_on(io.pwrite(&buf, 0))?;
        IO::block_on(io.fsync())?;
        io.close();
        // Open as sealed segment
        let seg = VlogSegment::<IO>::open_at(
            id, path.clone(), bytes.len() as u64, Vec::new(), self.io_flags,
        )?;
        // Scan entries and rebuild index
        if let Some(ref mut idx) = self.index {
            for (offset, raw) in seg.iter_raw_entries()? {
                if let Some(key) = key_fn(&raw) {
                    let ptr = VlogPtr {
                        segment_id: id,
                        offset,
                        length: raw.payload.len() as u32
                            + super::vlog::VLOG_RAW_ENTRY_OVERHEAD as u32,
                    };
                    idx.insert(key, ptr);
                }
            }
        }
        self.sealed_segments.insert(id, seg);
        Ok(Some(VlogSegmentMeta {
            segment_id: id,
            path,
            views: Vec::new(),
            total_size: bytes.len() as u64,
        }))
    }

    /// Append pre-encoded raw vlog entry bytes to the active segment.
    ///
    /// Used by the IR record view-change path to append delta entries
    /// received from the leader. The bytes are already in wire format
    /// (produced by `encode_raw_entry` / `encode_memtable_as_segment`).
    ///
    /// This does NOT touch the memtable or sealed segments:
    /// - Memtable is for the live write path (`put()` → `seal_view()`)
    /// - Sealed segments are for persisted view-change payloads
    ///   (`persist_sealed_segment`)
    /// - This method appends to the active vlog, which is tracked by
    ///   the manifest's `active_write_offset` and uploaded by
    ///   `upload_active_segments` in `sync_to_remote`
    ///
    /// `key_fn` reconstructs `K` from raw entry headers for index rebuild.
    pub(crate) fn append_to_active<F>(
        &mut self,
        bytes: &[u8],
        key_fn: F,
    ) -> Result<(), StorageError>
    where
        K: Clone,
        F: Fn(&RawVlogEntry) -> Option<K>,
    {
        if bytes.is_empty() {
            return Ok(());
        }
        let segment_id = self.active_vlog.id;
        let (_base, entries) = self.active_vlog.append_raw_bytes(bytes)?;
        if let Some(ref mut idx) = self.index {
            for (file_offset, entry_len, raw) in entries {
                if let Some(key) = key_fn(&raw) {
                    let ptr = VlogPtr {
                        segment_id,
                        offset: file_offset,
                        length: entry_len,
                    };
                    idx.insert(key, ptr);
                }
            }
        }
        Ok(())
    }

    /// Clear all data: index, sealed segments, and SSTs.
    ///
    /// Used before full payload installs to release old segment memory.
    /// Without this, sealed segments accumulate indefinitely across view changes,
    /// causing unbounded memory growth.
    pub(crate) fn clear_all(&mut self) {
        if let Some(ref mut idx) = self.index {
            idx.clear();
        }
        self.sealed_segments.clear();
        self.sst_readers.clear();
        self.sst_metas.clear();
        self.memtable.mem_clear();
        self.entry_count = 0;
    }

    /// Write the current in-memory index to a new SST file.
    ///
    /// Used after persist_sealed_segment in full-reset installs so that
    /// clones (which rebuild the index from SSTs, not vlog scans) can
    /// find entries from imported sealed segments.
    pub(crate) fn flush_index_to_sst(&mut self) -> Result<(), StorageError>
    where
        K: Serialize + DeserializeOwned + Clone,
    {
        if let Some(ref idx) = self.index {
            let snapshot = idx.clone();
            self.write_sst(&snapshot)?;
        }
        Ok(())
    }

    /// Replace the active vlog with a fresh empty segment.
    ///
    /// Used after clear_all in full-reset installs so that stale data
    /// in the old active segment is not uploaded to S3.
    pub(crate) fn reset_active(&mut self) -> Result<(), StorageError> {
        let new_id = self.next_segment_id;
        self.next_segment_id += 1;
        let new_path = self.vlog_path(new_id);
        let old = std::mem::replace(
            &mut self.active_vlog,
            VlogSegment::<IO>::open(new_id, new_path, self.io_flags)?,
        );
        old.close();
        Ok(())
    }

    /// Clear the memtable.
    pub(crate) fn clear_memtable(&mut self) {
        self.memtable.mem_clear();
    }
}

#[cfg(test)]
impl<K: Ord, V, IO: DiskIo, M: Memtable<K, V>> VlogLsm<K, V, IO, M> {
    pub(crate) fn index(&self) -> &BTreeMap<K, VlogPtr> {
        self.index
            .as_ref()
            .expect("index() requires IndexMode::InMemory")
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

    fn open_test_lsm(dir: &Path) -> VlogLsm<String, String, MemoryIo> {
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
            Vec::new(),
            0,
            IndexMode::InMemory,
        )
        .unwrap()
    }

    fn open_test_lsm_sst_only(dir: &Path) -> VlogLsm<String, String, MemoryIo> {
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
            Vec::new(),
            0,
            IndexMode::SstOnly,
        )
        .unwrap()
    }

    #[test]
    fn get_missing_key_returns_none() {
        let dir = MemoryIo::temp_path();
        let lsm = open_test_lsm(&dir);
        assert_eq!(lsm.get(&"missing".to_string()).unwrap(), None);
    }

    #[test]
    fn put_and_get_from_memtable() {
        let dir = MemoryIo::temp_path();
        let mut lsm = open_test_lsm(&dir);

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
    fn seal_reads_from_index() {
        let dir = MemoryIo::temp_path();
        let mut lsm = open_test_lsm(&dir);

        lsm.put("k_a".into(), "val_a".into());
        lsm.put("k_b".into(), "val_b".into());

        // Seal #1: memtable flushed to vlog + SST + index. min_vlog_size=0 forces rotation.
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(1);

        // Reads go through index (complete in-memory map).
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));

        lsm.put("k_c".into(), "val_c".into());
        lsm.put("k_d".into(), "val_d".into());

        assert_eq!(lsm.get(&"k_c".into()).unwrap(), Some("val_c".into()));

        // Seal #2: index accumulates (k_a, k_b from seal #1 + k_c, k_d).
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(2);

        // All reads via index (never cleared).
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));
        assert_eq!(lsm.get(&"k_c".into()).unwrap(), Some("val_c".into()));
        assert_eq!(lsm.get(&"k_d".into()).unwrap(), Some("val_d".into()));
    }

    #[test]
    fn multi_seal_mixed_read_sources() {
        let dir = MemoryIo::temp_path();
        let mut lsm = open_test_lsm(&dir);

        // (1) Missing key.
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), None);

        // (2) Put k_a, k_b → memtable reads.
        lsm.put("k_a".into(), "val_a".into());
        lsm.put("k_b".into(), "val_b".into());
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));

        // (3) Seal #1: vlog rotated, index accumulates.
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(1);
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));

        // Put k_c, k_d into new segment.
        lsm.put("k_c".into(), "val_c".into());
        lsm.put("k_d".into(), "val_d".into());
        assert_eq!(lsm.get(&"k_c".into()).unwrap(), Some("val_c".into()));

        // (4) Seal #2: index now has k_a..k_d.
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(2);
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));
        assert_eq!(lsm.get(&"k_c".into()).unwrap(), Some("val_c".into()));
        assert_eq!(lsm.get(&"k_d".into()).unwrap(), Some("val_d".into()));

        // (5) Put k_e, verify mixed reads (memtable + index).
        lsm.put("k_e".into(), "val_e".into());
        assert_eq!(lsm.get(&"k_e".into()).unwrap(), Some("val_e".into())); // memtable
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into())); // index
        assert_eq!(lsm.get(&"k_c".into()).unwrap(), Some("val_c".into())); // index

        // (6) Seal #3: index now has k_a..k_e.
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(3);
        assert_eq!(lsm.get(&"k_e".into()).unwrap(), Some("val_e".into())); // index
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into())); // index
        assert_eq!(lsm.get(&"k_d".into()).unwrap(), Some("val_d".into())); // index

        // (7) Missing key still returns None.
        assert_eq!(lsm.get(&"missing".into()).unwrap(), None);
    }

    #[test]
    fn seal_writes_sst_and_tracks_metadata() {
        let dir = MemoryIo::temp_path();
        let mut lsm = open_test_lsm(&dir);

        assert_eq!(lsm.sst_metas.len(), 0);
        assert_eq!(lsm.next_sst_id, 0);
        let k_a: String = "k_a".into();
        assert!(lsm.memtable.mem_get(&k_a).is_none());
        assert!(!lsm.index().contains_key(&k_a));

        lsm.put("k_a".into(), "val_a".into());
        assert!(lsm.memtable.mem_get(&k_a).is_some());

        lsm.seal_view(u64::MAX, test_header_fn).unwrap();
        lsm.start_view(1);

        // After seal, SST written with 1 entry.
        assert_eq!(lsm.sst_metas.len(), 1);
        assert_eq!(lsm.sst_metas[0].num_entries, 1);
        assert_eq!(lsm.next_sst_id, 1);
        // Key still accessible via index.
        assert!(lsm.index().contains_key(&k_a));

        lsm.put("k_b".into(), "val_b".into());
        lsm.seal_view(u64::MAX, test_header_fn).unwrap();
        lsm.start_view(2);

        // Second seal writes second SST with only the new entry.
        assert_eq!(lsm.sst_metas.len(), 2);
        assert_eq!(lsm.sst_metas[1].num_entries, 1);
        assert_eq!(lsm.next_sst_id, 2);
    }

    #[test]
    fn sst_only_get_after_seal() {
        let dir = MemoryIo::temp_path();
        let mut lsm = open_test_lsm_sst_only(&dir);

        lsm.put("k_a".into(), "val_a".into());
        lsm.put("k_b".into(), "val_b".into());

        // Before seal, reads come from memtable.
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));

        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(1);

        // After seal in SstOnly mode, reads go through SSTs.
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));
        assert_eq!(lsm.get(&"missing".into()).unwrap(), None);
    }

    #[test]
    fn sst_only_multi_seal_reads() {
        let dir = MemoryIo::temp_path();
        let mut lsm = open_test_lsm_sst_only(&dir);

        lsm.put("k_a".into(), "val_a".into());
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(1);

        lsm.put("k_b".into(), "val_b".into());
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(2);

        // Keys from both seals accessible via SSTs.
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));
        assert_eq!(lsm.get(&"missing".into()).unwrap(), None);
    }

    #[test]
    fn sst_only_mixed_memtable_and_sst_reads() {
        let dir = MemoryIo::temp_path();
        let mut lsm = open_test_lsm_sst_only(&dir);

        lsm.put("k_a".into(), "val_a".into());
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(1);

        // k_b is in memtable, k_a is in SST.
        lsm.put("k_b".into(), "val_b".into());
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
        assert_eq!(lsm.get(&"k_b".into()).unwrap(), Some("val_b".into()));
    }

    #[test]
    fn sst_only_range_get_first_after_seal() {
        let dir = MemoryIo::temp_path();
        let mut lsm = open_test_lsm_sst_only(&dir);

        lsm.put("k_a".into(), "val_a".into());
        lsm.put("k_c".into(), "val_c".into());
        lsm.put("k_e".into(), "val_e".into());
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(1);

        // Exact match.
        assert_eq!(
            lsm.range_get_first(&"k_a".into()).unwrap(),
            Some(("k_a".into(), "val_a".into()))
        );

        // Between keys: k_b -> k_c.
        assert_eq!(
            lsm.range_get_first(&"k_b".into()).unwrap(),
            Some(("k_c".into(), "val_c".into()))
        );

        // After all keys.
        assert_eq!(lsm.range_get_first(&"k_z".into()).unwrap(), None);
    }

    #[test]
    fn sst_only_range_get_first_multi_seal() {
        let dir = MemoryIo::temp_path();
        let mut lsm = open_test_lsm_sst_only(&dir);

        lsm.put("k_a".into(), "val_a".into());
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(1);

        lsm.put("k_c".into(), "val_c".into());
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(2);

        // Searches across SSTs from different seals.
        assert_eq!(
            lsm.range_get_first(&"k_b".into()).unwrap(),
            Some(("k_c".into(), "val_c".into()))
        );
        assert_eq!(
            lsm.range_get_first(&"k_a".into()).unwrap(),
            Some(("k_a".into(), "val_a".into()))
        );
    }

    #[test]
    fn sst_only_range_get_first_mixed_memtable_and_sst() {
        let dir = MemoryIo::temp_path();
        let mut lsm = open_test_lsm_sst_only(&dir);

        lsm.put("k_c".into(), "val_c".into());
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(1);

        // k_a is in memtable (smaller than k_c in SST).
        lsm.put("k_a".into(), "val_a".into());
        assert_eq!(
            lsm.range_get_first(&"k_a".into()).unwrap(),
            Some(("k_a".into(), "val_a".into()))
        );

        // k_b: not in memtable, falls to SST -> k_c.
        // But memtable has nothing >= k_b either, so SST wins.
        assert_eq!(
            lsm.range_get_first(&"k_b".into()).unwrap(),
            Some(("k_c".into(), "val_c".into()))
        );
    }

    #[test]
    #[should_panic(expected = "IndexMode::InMemory")]
    fn sst_only_index_panics() {
        let dir = MemoryIo::temp_path();
        let lsm = open_test_lsm_sst_only(&dir);
        let _ = lsm.index();
    }

    #[test]
    fn append_to_active_after_seal() {
        let dir = MemoryIo::temp_path();
        let mut lsm = open_test_lsm(&dir);

        // Write via memtable + seal (normal path).
        lsm.put("k_a".into(), "val_a".into());
        lsm.seal_view(0, test_header_fn).unwrap();
        lsm.start_view(1);
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));

        // Encode a new entry as raw bytes (simulating delta from view change).
        let raw_bytes = VlogSegment::<MemoryIo>::encode_raw_entry(
            0x80, 1, 42,
            &bitcode::serialize(&"val_appended".to_string()).unwrap(),
        );

        // Append to active segment.
        let offset_before = lsm.active_write_offset();
        lsm.append_to_active(&raw_bytes, |raw| {
            // Reconstruct key from the entry — use a fixed key for this test.
            if raw.id_number == 42 {
                Some("k_appended".to_string())
            } else {
                None
            }
        })
        .unwrap();

        // active_write_offset advanced.
        assert!(
            lsm.active_write_offset() > offset_before,
            "active_write_offset should advance after append"
        );

        // The appended entry is findable via index.
        let idx = lsm.index();
        assert!(
            idx.contains_key(&"k_appended".to_string()),
            "appended key should be in the index"
        );

        // The sealed entry from the first view is still findable.
        assert_eq!(lsm.get(&"k_a".into()).unwrap(), Some("val_a".into()));
    }
}
