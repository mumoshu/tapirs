mod manifest;
mod mvcc_backend;
mod prepare_cache;
pub mod types;
mod vlog;

#[cfg(test)]
mod tests;

use crate::ir::OpId;
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use crate::mvcc::disk::lsm::LsmTree;
use crate::mvcc::disk::memtable::Memtable;
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use manifest::UnifiedManifest;
use prepare_cache::PrepareCache;
use types::*;
use vlog::UnifiedVlogSegment;

pub use mvcc_backend::UnifiedMvccBackend;

/// Default prepare cache capacity.
const DEFAULT_PREPARE_CACHE_CAPACITY: usize = 1024;

/// Default minimum VLog segment size before starting a new file (256 KB).
const DEFAULT_MIN_VIEW_VLOG_SIZE: u64 = 256 * 1024;

/// The unified storage engine backing both IR record persistence
/// and MVCC value storage through a shared VLog.
///
/// Operates on opaque byte vectors for user keys and values in the VLog,
/// but uses typed `K` for the in-memory MVCC memtable index.
pub struct UnifiedStore<K: Ord, IO: DiskIo> {
    /// MVCC memtable: current view's committed values + read timestamps.
    mvcc_memtable: Memtable<K, Timestamp>,

    /// MVCC SSTs from sealed views.
    mvcc_tree: LsmTree<IO>,

    /// Active VLog segment for the current view.
    active_vlog: UnifiedVlogSegment<IO>,

    /// Sealed VLog segments (immutable, one per view group).
    sealed_vlog_segments: BTreeMap<u64, UnifiedVlogSegment<IO>>,

    /// Maps transaction_id → in-memory Prepare payload.
    /// Populated by `register_prepare()`, consumed by MVCC reads.
    prepare_registry: BTreeMap<OccTransactionId, Arc<CachedPrepare>>,

    /// LRU cache for deserialized CO::Prepare payloads from sealed VLog.
    prepare_cache: PrepareCache,

    /// Current view number.
    current_view: u64,

    /// Persisted manifest.
    manifest: UnifiedManifest,

    /// IR overlay: current view's IR entries (in-memory only).
    ir_overlay: BTreeMap<OpId, IrMemEntry>,

    /// IR base: maps OpId → IrSstEntry for the sealed IR base record.
    /// In a full implementation this would be an on-disk IR SST.
    /// For now, we use an in-memory BTreeMap as a stepping stone.
    ir_base: BTreeMap<OpId, IrSstEntry>,

    /// Base directory for all on-disk files.
    base_dir: PathBuf,

    /// I/O flags.
    io_flags: OpenFlags,

    /// Minimum VLog segment size before starting a new file.
    min_view_vlog_size: u64,

    /// Number of VLog reads performed (for testing LRU cache effectiveness).
    vlog_read_count: u64,

    /// Number of entries written to the current view's VLog (for view seal).
    current_view_entry_count: u32,
}

impl<K: Ord + Clone, IO: DiskIo> UnifiedStore<K, IO> {
    /// Open or create a unified store at the given directory.
    pub fn open(base_dir: PathBuf) -> Result<Self, StorageError> {
        Self::open_with_options(base_dir, DEFAULT_MIN_VIEW_VLOG_SIZE)
    }

    /// Open with custom minimum VLog segment size.
    pub fn open_with_options(
        base_dir: PathBuf,
        min_view_vlog_size: u64,
    ) -> Result<Self, StorageError> {
        let io_flags = OpenFlags {
            create: true,
            direct: false,
        };

        IO::create_dir_all(&base_dir)?;

        let manifest = match UnifiedManifest::load::<IO>(&base_dir)? {
            Some(m) => m,
            None => UnifiedManifest::new(),
        };

        // Open sealed VLog segments
        let mut sealed_vlog_segments = BTreeMap::new();
        for seg_meta in &manifest.sealed_vlog_segments {
            let seg = UnifiedVlogSegment::<IO>::open_at(
                seg_meta.segment_id,
                seg_meta.path.clone(),
                seg_meta.total_size,
                seg_meta.views.clone(),
                io_flags,
            )?;
            sealed_vlog_segments.insert(seg_meta.segment_id, seg);
        }

        // Open or create active VLog segment
        let active_path = base_dir.join(format!(
            "vlog_seg_{:04}.dat",
            manifest.active_segment_id
        ));
        let active_vlog = UnifiedVlogSegment::<IO>::open_at(
            manifest.active_segment_id,
            active_path,
            manifest.active_write_offset,
            Vec::new(),
            io_flags,
        )?;

        // Restore MVCC LSM tree
        let mvcc_tree = LsmTree::<IO>::restore(
            base_dir.clone(),
            manifest.mvcc_l0_sstables.clone(),
            manifest.mvcc_l1_sstables.clone(),
            manifest.next_sst_id,
            io_flags,
        );

        Ok(Self {
            mvcc_memtable: Memtable::new(),
            mvcc_tree,
            active_vlog,
            sealed_vlog_segments,
            prepare_registry: BTreeMap::new(),
            prepare_cache: PrepareCache::new(DEFAULT_PREPARE_CACHE_CAPACITY),
            current_view: manifest.current_view,
            manifest,
            ir_overlay: BTreeMap::new(),
            ir_base: BTreeMap::new(),
            base_dir,
            io_flags,
            min_view_vlog_size,
            vlog_read_count: 0,
            current_view_entry_count: 0,
        })
    }

    pub fn current_view(&self) -> u64 {
        self.current_view
    }

    pub fn vlog_read_count(&self) -> u64 {
        self.vlog_read_count
    }

    pub fn sealed_vlog_segments(&self) -> &BTreeMap<u64, UnifiedVlogSegment<IO>> {
        &self.sealed_vlog_segments
    }

    /// Insert an IR entry into the overlay.
    pub fn insert_ir_entry(&mut self, op_id: OpId, entry: IrMemEntry) {
        self.ir_overlay.insert(op_id, entry);
    }

    /// Look up an IR entry by OpId (overlay first, then base).
    pub fn ir_entry(&self, op_id: &OpId) -> Option<IrEntryRef<'_>> {
        if let Some(mem_entry) = self.ir_overlay.get(op_id) {
            Some(IrEntryRef::Overlay(mem_entry))
        } else {
            self.ir_base.get(op_id).map(IrEntryRef::Base)
        }
    }

    /// Look up an IR base SST entry by OpId.
    pub fn lookup_ir_base_entry(&self, op_id: OpId) -> Option<&IrSstEntry> {
        self.ir_base.get(&op_id)
    }

    /// Register a prepare in the in-memory registry.
    /// Called from `MvccBackend::register_prepare`.
    pub fn register_prepare_raw(
        &mut self,
        txn_id: OccTransactionId,
        prepare: Arc<CachedPrepare>,
    ) {
        self.prepare_registry.insert(txn_id, prepare);
    }

    /// Remove a prepare from the in-memory registry.
    pub fn unregister_prepare(&mut self, txn_id: &OccTransactionId) {
        self.prepare_registry.remove(txn_id);
    }

    /// Resolve a value from the prepare registry (InMemory path).
    pub fn resolve_in_memory(
        &self,
        txn_id: &OccTransactionId,
        write_index: u16,
    ) -> Option<&(Vec<u8>, Vec<u8>)> {
        self.prepare_registry
            .get(txn_id)
            .and_then(|p| p.write_set.get(write_index as usize))
    }

    /// Resolve a value from a sealed VLog (OnDisk path).
    /// Uses the prepare cache to avoid repeated reads.
    pub fn resolve_on_disk(
        &mut self,
        ptr: &UnifiedVlogPrepareValuePtr,
    ) -> Result<Arc<CachedPrepare>, StorageError> {
        let key_seg = ptr.prepare_ptr.segment_id;
        let key_off = ptr.prepare_ptr.offset;

        // Check cache first
        if let Some(cached) = self.prepare_cache.get(key_seg, key_off) {
            return Ok(cached);
        }

        // Cache miss: read from VLog (sealed or active)
        self.vlog_read_count += 1;

        // Check sealed segments first, then the active segment (which may
        // contain sealed view entries when the segment is smaller than
        // min_view_vlog_size and hasn't been rotated yet).
        let segment = self
            .sealed_vlog_segments
            .get(&key_seg)
            .or(if self.active_vlog.id == key_seg {
                Some(&self.active_vlog)
            } else {
                None
            })
            .ok_or_else(|| StorageError::Codec(format!(
                "VLog segment {key_seg} not found for prepare resolution"
            )))?;

        let prepare = segment.read_prepare(&ptr.prepare_ptr)?;
        let cached = Arc::new(prepare);
        self.prepare_cache.insert(key_seg, key_off, cached.clone());
        Ok(cached)
    }

    /// Get a reference to the MVCC memtable.
    pub fn mvcc_memtable(&self) -> &Memtable<K, Timestamp> {
        &self.mvcc_memtable
    }

    /// Get a mutable reference to the MVCC memtable.
    pub fn mvcc_memtable_mut(&mut self) -> &mut Memtable<K, Timestamp> {
        &mut self.mvcc_memtable
    }

    /// Get a reference to the MVCC LSM tree.
    pub fn mvcc_tree(&self) -> &LsmTree<IO> {
        &self.mvcc_tree
    }

    /// Look up an MVCC entry in the memtable by key bytes and timestamp.
    pub fn mvcc_entry_from_memtable(
        &self,
        key: &K,
        ts: Timestamp,
    ) -> Option<&UnifiedLsmEntry>
    where
        K: Clone,
    {
        // The memtable stores LsmEntry (from the existing memtable module),
        // but our unified store wraps it. For now, we need a separate
        // unified memtable. This is a placeholder that will be replaced
        // with the actual unified memtable integration.
        let _ = (key, ts);
        None
    }

    /// Seal the current view: write VLog entries, flush MVCC memtable → SST,
    /// update manifest, advance to next view.
    pub fn seal_current_view(&mut self) -> Result<(), StorageError>
    where
        K: serde::Serialize + Clone,
    {
        // 1. Write all finalized overlay entries to VLog
        let finalized_entries: Vec<(OpId, VlogEntryType, IrPayloadInline)> = self
            .ir_overlay
            .iter()
            .filter(|(_, entry)| matches!(entry.state, IrState::Finalized(_)))
            .map(|(op_id, entry)| (*op_id, entry.entry_type, entry.payload.clone()))
            .collect();

        let mut vlog_ptrs = Vec::new();
        if !finalized_entries.is_empty() {
            let entry_refs: Vec<(OpId, VlogEntryType, &IrPayloadInline)> = finalized_entries
                .iter()
                .map(|(op, et, p)| (*op, *et, p))
                .collect();
            vlog_ptrs = self.active_vlog.append_batch(&entry_refs)?;
        }

        // 2. Build IR base entries from the VLog pointers
        for (i, (op_id, entry_type, _)) in finalized_entries.iter().enumerate() {
            self.ir_base.insert(
                *op_id,
                IrSstEntry {
                    entry_type: *entry_type,
                    vlog_ptr: vlog_ptrs[i],
                },
            );
        }

        // 3. Sync VLog
        self.active_vlog.sync()?;

        // 4. Record view range in active VLog segment
        self.active_vlog
            .finish_view(finalized_entries.len() as u32);

        // 5. Flush MVCC memtable → SST (using existing LsmTree)
        if !self.mvcc_memtable.is_empty() {
            // The existing LsmTree expects Memtable<K, TS> with LsmEntry values.
            // For now, we flush an empty memtable to create the SST structure.
            // The actual unified memtable integration will replace this.
            // TODO: Implement unified memtable → SST flush
        }

        // 6. Decide whether to seal the VLog segment or continue
        let segment_size = self.active_vlog.write_offset();
        if segment_size >= self.min_view_vlog_size {
            // Seal current segment, start new one
            let sealed_id = self.active_vlog.id;
            let sealed_path = self.active_vlog.path().clone();
            let sealed_views = self.active_vlog.views.clone();
            let sealed_size = self.active_vlog.write_offset();

            // Record in manifest
            self.manifest.sealed_vlog_segments.push(VlogSegmentMeta {
                segment_id: sealed_id,
                path: sealed_path.clone(),
                views: sealed_views.clone(),
                total_size: sealed_size,
            });

            // Move active to sealed
            let old_active = std::mem::replace(
                &mut self.active_vlog,
                {
                    let new_id = self.manifest.next_segment_id;
                    self.manifest.next_segment_id += 1;
                    let new_path = self
                        .base_dir
                        .join(format!("vlog_seg_{new_id:04}.dat"));
                    UnifiedVlogSegment::<IO>::open(new_id, new_path, self.io_flags)?
                },
            );
            self.sealed_vlog_segments.insert(
                sealed_id,
                UnifiedVlogSegment::<IO>::open_at(
                    sealed_id,
                    sealed_path,
                    sealed_size,
                    sealed_views,
                    self.io_flags,
                )?,
            );
            old_active.close();
        }

        // 7. Update manifest
        self.current_view += 1;
        self.manifest.current_view = self.current_view;
        self.manifest.active_segment_id = self.active_vlog.id;
        self.manifest.active_write_offset = self.active_vlog.write_offset();
        self.manifest.save::<IO>(&self.base_dir)?;

        // 8. Clear overlay and prepare registry
        self.ir_overlay.clear();
        self.prepare_registry.clear();
        self.current_view_entry_count = 0;

        // 9. Start new view in active segment
        self.active_vlog.start_view(self.current_view);

        Ok(())
    }

    /// Extract only finalized entries from the overlay (for leader merge simulation).
    pub fn extract_finalized_entries(&self) -> Vec<(OpId, IrMemEntry)> {
        self.ir_overlay
            .iter()
            .filter(|(_, entry)| matches!(entry.state, IrState::Finalized(_)))
            .map(|(op_id, entry)| (*op_id, entry.clone()))
            .collect()
    }

    /// Install a merged record as the new IR base.
    /// All entries are marked Finalized(target_view).
    pub fn install_merged_record(
        &mut self,
        entries: Vec<(OpId, IrMemEntry)>,
        target_view: u64,
    ) -> Result<(), StorageError> {
        // Clear old base and install new one
        self.ir_base.clear();

        // Write entries to VLog
        let entry_refs: Vec<(OpId, VlogEntryType, &IrPayloadInline)> = entries
            .iter()
            .map(|(op, entry)| (*op, entry.entry_type, &entry.payload))
            .collect();

        if !entry_refs.is_empty() {
            let ptrs = self.active_vlog.append_batch(&entry_refs)?;
            for (i, (op_id, entry)) in entries.iter().enumerate() {
                self.ir_base.insert(
                    *op_id,
                    IrSstEntry {
                        entry_type: entry.entry_type,
                        vlog_ptr: ptrs[i],
                    },
                );
            }
        }

        self.current_view = target_view;
        self.manifest.current_view = target_view;
        self.ir_overlay.clear();
        self.prepare_registry.clear();

        Ok(())
    }
}

/// Reference to an IR entry (either in overlay or base).
pub enum IrEntryRef<'a> {
    Overlay(&'a IrMemEntry),
    Base(&'a IrSstEntry),
}
