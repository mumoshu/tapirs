pub mod cli;
mod ir_record;
mod manifest;
mod prepare_cache;
pub mod types;
pub(crate) mod unified_memtable;
mod vlog;

mod mvcc_impl;
mod tapirstore;

#[cfg(test)]
mod tests;

use crate::ir::OpId;
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use crate::mvcc::disk::memtable::Memtable;
use crate::occ::{Transaction, TransactionId as OccTransactionId};
use crate::tapir::{ShardNumber, Timestamp};
use crate::tapirstore::{MinPrepareTimes, RecordDeltaDuringView, TransactionLog};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ir_record::IrRecord;
use manifest::UnifiedManifest;
use prepare_cache::PrepareCache;
use types::*;
use unified_memtable::UnifiedMemtable;
use vlog::UnifiedVlogSegment;

/// Default prepare cache capacity.
const DEFAULT_PREPARE_CACHE_CAPACITY: usize = 1024;

/// Default minimum VLog segment size before starting a new file (256 KB).
const DEFAULT_MIN_VIEW_VLOG_SIZE: u64 = 256 * 1024;

/// The unified storage engine backing both IR record persistence
/// and MVCC value storage through a shared VLog.
///
/// # Data lifecycle
///
/// `K` and `V` are stored **typed** in memory (`ir_overlay`,
/// `prepare_registry`) so that prepared transactions can be inspected,
/// indexed, and committed without a serialization round-trip.
/// Serialization to bytes happens exactly once at `seal_current_view()`
/// time when the data is written to the VLog.  Deserialization from the
/// VLog happens only for cross-view reads (rare), and the result is
/// cached in `prepare_cache` to avoid repeated VLog I/O.
///
/// Because of this, `K` and `V` must be `Send + Sync` (required by
/// `Arc<CachedPrepare<K, V>>` which must be `Send` for the `MvccBackend`
/// trait), and `Serialize + DeserializeOwned` (for VLog persistence).
///
/// # Value indirection
///
/// Committed values are NOT duplicated in the MVCC index.  Instead,
/// each MVCC entry (`UnifiedLsmEntry`) holds a `ValueLocation` that
/// points either to the `prepare_registry` (current view) or to a
/// CO::Prepare entry in the VLog (sealed views).  Reading a committed
/// value always goes through `resolve_in_memory` or `resolve_on_disk`.
pub struct UnifiedStore<K: Ord, V, IO: DiskIo> {
    /// MVCC memtable: current view's committed values + read timestamps.
    mvcc_memtable: Memtable<K, Timestamp>,

    /// Unified MVCC memtable using UnifiedLsmEntry with ValueLocation.
    unified_memtable: UnifiedMemtable<K>,

    /// Active VLog segment for the current view.
    active_vlog: UnifiedVlogSegment<IO>,

    /// Sealed VLog segments (immutable, one per view group).
    sealed_vlog_segments: BTreeMap<u64, UnifiedVlogSegment<IO>>,

    /// Current view's typed Prepare payloads, keyed by transaction_id.
    ///
    /// This is the fast path for value resolution: MVCC entries with
    /// `ValueLocation::InMemory` look up values here instead of reading
    /// the VLog.  Cleared at `seal_current_view()` because the data moves
    /// to the VLog and future reads go through `prepare_cache` instead.
    prepare_registry: BTreeMap<OccTransactionId, Arc<CachedPrepare<K, V>>>,

    /// LRU cache for Prepare payloads deserialized from sealed VLog segments.
    ///
    /// Only consulted for `ValueLocation::OnDisk` reads (cross-view commits
    /// or reads after seal).  Uses `RefCell` for interior mutability because
    /// `get()` takes `&self`.
    prepare_cache: RefCell<PrepareCache<K, V>>,

    /// Current view number.
    current_view: u64,

    /// Persisted manifest.
    manifest: UnifiedManifest,

    /// IR record: overlay (current view), base (sealed views), and
    /// prepare VLog index for cross-view commits.
    ir_record: IrRecord<K, V>,

    /// Base directory for all on-disk files.
    base_dir: PathBuf,

    /// I/O flags.
    io_flags: OpenFlags,

    /// Minimum VLog segment size before starting a new file.
    min_view_vlog_size: u64,

    /// Number of VLog reads performed (for testing LRU cache effectiveness).
    /// Uses Cell for interior mutability (get() takes &self).
    vlog_read_count: Cell<u64>,

    /// Number of entries written to the current view's VLog (for view seal).
    current_view_entry_count: u32,

    /// Transaction log tracking committed/aborted outcomes.
    transaction_log: TransactionLog,

    /// Min-prepare-time state machine (tentative + finalized thresholds).
    min_prepare_times: MinPrepareTimes,

    /// CDC delta storage for resharding catch-up.
    record_delta_during_view: RecordDeltaDuringView<K, V>,
}

impl<K: Ord + Clone, V, IO: DiskIo> UnifiedStore<K, V, IO> {
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
        let mut active_vlog = UnifiedVlogSegment::<IO>::open_at(
            manifest.active_segment_id,
            active_path,
            manifest.active_write_offset,
            Vec::new(),
            io_flags,
        )?;

        let current_view = manifest.current_view;

        // Start tracking the current view in the active VLog segment.
        // This ensures finish_view() at seal time has a ViewRange to update.
        active_vlog.start_view(current_view);

        Ok(Self {
            mvcc_memtable: Memtable::new(),
            unified_memtable: UnifiedMemtable::new(),
            active_vlog,
            sealed_vlog_segments,
            prepare_registry: BTreeMap::new(),
            prepare_cache: RefCell::new(PrepareCache::new(DEFAULT_PREPARE_CACHE_CAPACITY)),
            current_view,
            manifest,
            ir_record: IrRecord::new(),
            base_dir,
            io_flags,
            min_view_vlog_size,
            vlog_read_count: Cell::new(0),
            current_view_entry_count: 0,
            transaction_log: TransactionLog::new(),
            min_prepare_times: MinPrepareTimes::new(),
            record_delta_during_view: RecordDeltaDuringView::new(),
        })
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn current_view(&self) -> u64 {
        self.current_view
    }

    pub fn vlog_read_count(&self) -> u64 {
        self.vlog_read_count.get()
    }

    pub fn sealed_vlog_segments(&self) -> &BTreeMap<u64, UnifiedVlogSegment<IO>> {
        &self.sealed_vlog_segments
    }

    /// Get the active VLog segment's ID.
    pub fn active_vlog_id(&self) -> u64 {
        self.active_vlog.id
    }

    /// Get the active VLog segment's write offset (bytes written).
    pub fn active_vlog_write_offset(&self) -> u64 {
        self.active_vlog.write_offset()
    }

    /// Get the active VLog segment's view ranges.
    pub fn active_vlog_views(&self) -> &[ViewRange] {
        &self.active_vlog.views
    }

    /// Dump all entries from a VLog segment (by segment ID).
    /// Returns entries from either the active or a sealed segment.
    pub fn dump_vlog_segment(
        &self,
        segment_id: u64,
    ) -> Result<Vec<(u64, crate::ir::OpId, VlogEntryType, IrPayloadInline<K, V>)>, StorageError>
    where
        K: serde::de::DeserializeOwned,
        V: serde::de::DeserializeOwned,
    {
        if self.active_vlog.id == segment_id {
            self.active_vlog.iter_entries()
        } else if let Some(seg) = self.sealed_vlog_segments.get(&segment_id) {
            seg.iter_entries()
        } else {
            Err(StorageError::Codec(format!(
                "VLog segment {segment_id} not found"
            )))
        }
    }

    /// Iterate over all IR overlay entries.
    pub fn ir_overlay_entries(&self) -> impl Iterator<Item = (&OpId, &IrMemEntry<K, V>)> {
        self.ir_record.ir_overlay.iter()
    }

    /// Insert an IR entry into the overlay.
    pub fn insert_ir_entry(&mut self, op_id: OpId, entry: IrMemEntry<K, V>) {
        self.ir_record.ir_overlay.insert(op_id, entry);
    }

    /// Look up an IR entry by OpId (overlay first, then base).
    pub fn ir_entry(&self, op_id: &OpId) -> Option<IrEntryRef<'_, K, V>> {
        if let Some(mem_entry) = self.ir_record.ir_overlay.get(op_id) {
            Some(IrEntryRef::Overlay(mem_entry))
        } else {
            self.ir_record.ir_base.get(op_id).map(IrEntryRef::Base)
        }
    }

    /// Look up an IR base SST entry by OpId.
    pub fn lookup_ir_base_entry(&self, op_id: OpId) -> Option<&IrSstEntry> {
        self.ir_record.ir_base.get(&op_id)
    }

    /// Register a typed Prepare payload in the in-memory registry.
    ///
    /// Must be called before `commit_prepared` so that
    /// the commit path can create `ValueLocation::InMemory` entries
    /// pointing into this registry.  If the view is sealed before
    /// the commit arrives, the prepare is still accessible via
    /// `prepare_vlog_index` + `resolve_on_disk`.
    fn register_prepare_raw(
        &mut self,
        txn_id: OccTransactionId,
        prepare: Arc<CachedPrepare<K, V>>,
    ) {
        self.prepare_registry.insert(txn_id, prepare);
    }

    /// Register a prepared transaction for future zero-copy commit.
    ///
    /// Extracts typed K, V from the `Transaction` and stores them in
    /// the prepare_registry.  Must be called before the corresponding
    /// `commit_prepared` so that committed MVCC entries
    /// can point into the registry via `ValueLocation::InMemory`.
    ///
    /// No serialization happens here — that is deferred to seal time.
    pub fn register_prepare(
        &mut self,
        txn_id: OccTransactionId,
        transaction: &Transaction<K, V, Timestamp>,
        commit_ts: Timestamp,
    ) where
        V: Clone,
    {
        let shard = ShardNumber(0); // Unified store serves one shard

        let read_set: Vec<(K, Timestamp)> = transaction
            .shard_read_set(shard)
            .map(|(k, ts)| (k.clone(), ts))
            .collect();

        let write_set: Vec<(K, Option<V>)> = transaction
            .shard_write_set(shard)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let scan_set: Vec<(K, K, Timestamp)> = transaction
            .shard_scan_set(shard)
            .map(|entry| {
                (
                    entry.start_key.clone(),
                    entry.end_key.clone(),
                    entry.timestamp,
                )
            })
            .collect();

        let prepare = Arc::new(CachedPrepare {
            transaction_id: txn_id,
            commit_ts,
            read_set,
            write_set,
            scan_set,
        });

        self.register_prepare_raw(txn_id, prepare);
    }

    /// Commit a prepared transaction by creating MVCC index entries.
    ///
    /// The `commit` timestamp is the **final** commit timestamp chosen by
    /// the TAPIR coordinator.  It may differ from `CachedPrepare::commit_ts`
    /// (the prepare-time proposal): when replicas return `Retry { proposed }`,
    /// the coordinator picks the maximum as the final timestamp.  MVCC
    /// entries must use this final timestamp so that reads at a given
    /// snapshot see the correct version.
    ///
    /// Two paths, tried in order:
    ///
    /// 1. **InMemory** — prepare is in the current view's `prepare_registry`.
    ///    MVCC entries get `ValueLocation::InMemory { txn_id, write_index }`.
    ///    This is the common case and avoids any VLog I/O.
    ///
    /// 2. **OnDisk** — prepare was sealed into the VLog (cross-view commit).
    ///    MVCC entries get `ValueLocation::OnDisk(ptr)` using the
    ///    `prepare_vlog_index`.  Value reads later go through the LRU cache.
    ///
    /// Returns `Err(StorageError::PrepareNotFound)` if neither the
    /// `prepare_registry` nor `prepare_vlog_index` has the transaction.
    pub fn commit_prepared(
        &mut self,
        txn_id: OccTransactionId,
        commit: Timestamp,
    ) -> Result<(), StorageError>
    where
        K: serde::de::DeserializeOwned,
        V: serde::de::DeserializeOwned,
    {
        // Clone the Arc to release the immutable borrow, allowing
        // subsequent unified_memtable_mut() calls.
        let prepare = self.prepare_registry.get(&txn_id).cloned();
        let cross_view_ptr = self.ir_record.prepare_vlog_index.get(&txn_id).copied();

        if let Some(prepare) = prepare {
            // InMemory path: prepare is in current view's prepare_registry
            for (i, (key, value)) in prepare.write_set.iter().enumerate() {
                let value_ref = if value.is_some() {
                    Some(ValueLocation::InMemory {
                        txn_id,
                        write_index: i as u16,
                    })
                } else {
                    None
                };

                self.unified_memtable_mut().insert(
                    key.clone(),
                    commit,
                    UnifiedLsmEntry {
                        value_ref,
                        last_read_ts: None,
                    },
                );
            }

            // Update read timestamps
            for (key, read) in &prepare.read_set {
                self.unified_memtable_mut()
                    .update_last_read(key, *read, commit.time);
            }
        } else if let Some(vlog_ptr) = cross_view_ptr {
            // OnDisk path: prepare is in a sealed VLog (cross-view commit)
            let cached = self.resolve_on_disk(&UnifiedVlogPrepareValuePtr {
                prepare_ptr: vlog_ptr,
                write_index: 0,
            })?;

            for (i, (key, value)) in cached.write_set.iter().enumerate() {
                let value_ref = if value.is_some() {
                    Some(ValueLocation::OnDisk(UnifiedVlogPrepareValuePtr {
                        prepare_ptr: vlog_ptr,
                        write_index: i as u16,
                    }))
                } else {
                    None
                };

                self.unified_memtable_mut().insert(
                    key.clone(),
                    commit,
                    UnifiedLsmEntry {
                        value_ref,
                        last_read_ts: None,
                    },
                );
            }

            // Update read timestamps
            for (key, read) in &cached.read_set {
                self.unified_memtable_mut()
                    .update_last_read(key, *read, commit.time);
            }
        } else {
            return Err(StorageError::PrepareNotFound {
                client_id: txn_id.client_id.0,
                txn_number: txn_id.number,
            });
        }

        Ok(())
    }

    /// Remove a prepare from the in-memory registry.
    pub fn unregister_prepare(&mut self, txn_id: &OccTransactionId) {
        self.prepare_registry.remove(txn_id);
    }

    /// Resolve a value from the prepare registry (InMemory path).
    ///
    /// Returns a reference to the typed `(key, Option<value>)` pair
    /// from the current view's `prepare_registry`.  Returns `None` if
    /// the transaction has been sealed or was never registered.
    pub fn resolve_in_memory(
        &self,
        txn_id: &OccTransactionId,
        write_index: u16,
    ) -> Option<&(K, Option<V>)> {
        self.prepare_registry
            .get(txn_id)
            .and_then(|p| p.write_set.get(write_index as usize))
    }

    /// Resolve a value from a sealed VLog (OnDisk path).
    ///
    /// Reads and deserializes the full CO::Prepare entry from the VLog,
    /// then caches it in `prepare_cache` so subsequent reads for the same
    /// transaction (or different write_set indices within it) avoid VLog I/O.
    ///
    /// Takes `&self` via interior mutability (`RefCell`/`Cell`) so it can
    /// be called from `get(&self)`.
    fn resolve_on_disk(
        &self,
        ptr: &UnifiedVlogPrepareValuePtr,
    ) -> Result<Arc<CachedPrepare<K, V>>, StorageError>
    where
        K: serde::de::DeserializeOwned,
        V: serde::de::DeserializeOwned,
    {
        let key_seg = ptr.prepare_ptr.segment_id;
        let key_off = ptr.prepare_ptr.offset;

        // Check cache first
        if let Some(cached) = self.prepare_cache.borrow_mut().get(key_seg, key_off) {
            return Ok(cached);
        }

        // Cache miss: read from VLog (sealed or active)
        self.vlog_read_count.set(self.vlog_read_count.get() + 1);

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
        self.prepare_cache.borrow_mut().insert(key_seg, key_off, cached.clone());
        Ok(cached)
    }

    /// Resolve a committed value from its `ValueLocation`.
    ///
    /// Both paths return typed `V` without the caller needing to know
    /// whether the value is in memory or on disk:
    ///
    /// - `InMemory` → zero-copy lookup in `prepare_registry` (current view)
    /// - `OnDisk` → VLog read + LRU cache lookup (sealed views)
    /// - `None` → delete tombstone or metadata-only entry (e.g. `commit_get`)
    pub fn resolve_value(&self, entry: &UnifiedLsmEntry) -> Result<Option<V>, StorageError>
    where
        K: serde::de::DeserializeOwned,
        V: Clone + serde::de::DeserializeOwned,
    {
        match &entry.value_ref {
            None => Ok(None),
            Some(ValueLocation::InMemory { txn_id, write_index }) => {
                match self.resolve_in_memory(txn_id, *write_index) {
                    Some((_key, value)) => Ok(value.clone()),
                    None => Ok(None),
                }
            }
            Some(ValueLocation::OnDisk(ptr)) => {
                let cached = self.resolve_on_disk(ptr)?;
                if let Some((_key, value)) =
                    cached.write_set.get(ptr.write_index as usize)
                {
                    Ok(value.clone())
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Get a reference to the unified MVCC memtable.
    pub(crate) fn unified_memtable(&self) -> &UnifiedMemtable<K> {
        &self.unified_memtable
    }

    /// Get a mutable reference to the unified MVCC memtable.
    pub(crate) fn unified_memtable_mut(&mut self) -> &mut UnifiedMemtable<K> {
        &mut self.unified_memtable
    }

    /// Seal the current view: serialize typed overlay entries to the VLog,
    /// convert `InMemory` MVCC entries to `OnDisk`, clear overlay and
    /// registry, advance to the next view.
    ///
    /// This is the only point where typed `K, V` are serialized to bytes.
    /// After seal, all value resolution for this view goes through
    /// `resolve_on_disk` (VLog read + `prepare_cache`).
    pub fn seal_current_view(&mut self) -> Result<(), StorageError>
    where
        K: serde::Serialize + Clone,
        V: serde::Serialize + Clone,
    {
        // 1. Write all finalized overlay entries to VLog
        let finalized_entries: Vec<(OpId, VlogEntryType, IrPayloadInline<K, V>)> = self
            .ir_record
            .ir_overlay
            .iter()
            .filter(|(_, entry)| matches!(entry.state, IrState::Finalized(_)))
            .map(|(op_id, entry)| (*op_id, entry.entry_type, entry.payload.clone()))
            .collect();

        let mut vlog_ptrs = Vec::new();
        if !finalized_entries.is_empty() {
            let entry_refs: Vec<(OpId, VlogEntryType, &IrPayloadInline<K, V>)> = finalized_entries
                .iter()
                .map(|(op, et, p)| (*op, *et, p))
                .collect();
            vlog_ptrs = self.active_vlog.append_batch(&entry_refs)?;
        }

        // 2. Build IR base entries and prepare_vlog_index from VLog pointers
        for (i, (op_id, entry_type, payload)) in finalized_entries.iter().enumerate() {
            self.ir_record.ir_base.insert(
                *op_id,
                IrSstEntry {
                    entry_type: *entry_type,
                    vlog_ptr: vlog_ptrs[i],
                },
            );
            // Index CO::Prepare entries by transaction_id for cross-view commit
            if *entry_type == VlogEntryType::Prepare
                && let IrPayloadInline::Prepare { transaction_id, .. } = payload
            {
                self.ir_record.prepare_vlog_index.insert(*transaction_id, vlog_ptrs[i]);
            }
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

        // 8. Convert unified memtable InMemory → OnDisk, then clear overlay + registry
        self.unified_memtable
            .convert_in_memory_to_on_disk(&self.ir_record.prepare_vlog_index);
        self.ir_record.ir_overlay.clear();
        self.prepare_registry.clear();
        self.current_view_entry_count = 0;

        // 9. Start new view in active segment
        self.active_vlog.start_view(self.current_view);

        Ok(())
    }

    /// Extract only finalized entries from the overlay (for leader merge simulation).
    pub fn extract_finalized_entries(&self) -> Vec<(OpId, IrMemEntry<K, V>)>
    where
        K: Clone,
        V: Clone,
    {
        self.ir_record
            .ir_overlay
            .iter()
            .filter(|(_, entry)| matches!(entry.state, IrState::Finalized(_)))
            .map(|(op_id, entry)| (*op_id, entry.clone()))
            .collect()
    }

    /// Install a merged record as the new IR base.
    /// All entries are marked Finalized(target_view).
    pub fn install_merged_record(
        &mut self,
        entries: Vec<(OpId, IrMemEntry<K, V>)>,
        target_view: u64,
    ) -> Result<(), StorageError>
    where
        K: serde::Serialize,
        V: serde::Serialize,
    {
        // Clear old base and install new one
        self.ir_record.ir_base.clear();

        // Write entries to VLog
        let entry_refs: Vec<(OpId, VlogEntryType, &IrPayloadInline<K, V>)> = entries
            .iter()
            .map(|(op, entry)| (*op, entry.entry_type, &entry.payload))
            .collect();

        if !entry_refs.is_empty() {
            let ptrs = self.active_vlog.append_batch(&entry_refs)?;
            for (i, (op_id, entry)) in entries.iter().enumerate() {
                self.ir_record.ir_base.insert(
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
        self.ir_record.ir_overlay.clear();
        self.prepare_registry.clear();
        // Note: unified_memtable is NOT cleared — committed entries from
        // previous views remain valid with OnDisk pointers.

        Ok(())
    }
}

/// Reference to an IR entry — either a typed in-memory overlay entry
/// or a byte-level base SST entry.
///
/// Callers must handle both variants: `Overlay` gives direct access to
/// typed payload fields, while `Base` only provides a VLog pointer
/// (requires a VLog read to access the payload).
pub enum IrEntryRef<'a, K, V> {
    Overlay(&'a IrMemEntry<K, V>),
    Base(&'a IrSstEntry),
}
