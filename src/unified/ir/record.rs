use crate::ir::OpId;
use crate::mvcc::disk::disk_io::DiskIo;
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use crate::unified::wisckeylsm::manifest::UnifiedManifest;
use crate::unified::wisckeylsm::vlog::UnifiedVlogSegment;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;

use super::types::UnifiedVlogPtr;

/// Entry type discriminator for IR VLog entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum VlogEntryType {
    /// CO::Prepare — consensus operation carrying the full transaction.
    Prepare = 0x01,
    /// IO::Commit — inconsistent operation that commits a prepared transaction.
    Commit = 0x02,
    /// IO::Abort — inconsistent operation that aborts a prepared transaction.
    Abort = 0x03,
    /// IO::QuorumRead — inconsistent operation for RO transaction slow path.
    QuorumRead = 0x04,
    /// IO::QuorumScan — inconsistent operation for RO scan slow path.
    QuorumScan = 0x05,
    /// CO::RaiseMinPrepareTime — consensus operation that raises the
    /// shard's minimum prepare timestamp.
    RaiseMinPrepareTime = 0x06,
}

/// IR memtable entry — lives only in the current view's in-memory overlay.
///
/// Generic over `K, V` because it wraps `IrPayloadInline<K, V>`.  There is
/// no `vlog_ptr` field: VLog writes are deferred to view seal time.  There
/// is no `modified_view` field: overlay entries always belong to the current
/// view, so the field would be redundant.
///
/// At seal time, each finalized `IrMemEntry` is serialized into the VLog
/// and replaced by an `IrSstEntry` (which *does* carry a `vlog_ptr`).
pub struct IrMemEntry<K, V> {
    /// Which IR operation type this entry represents.
    pub entry_type: VlogEntryType,
    /// Whether this entry is Tentative or Finalized.
    pub state: IrState,
    /// The full operation payload held inline in memory.
    pub payload: IrPayloadInline<K, V>,
}

impl<K: Debug, V: Debug> Debug for IrMemEntry<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrMemEntry")
            .field("entry_type", &self.entry_type)
            .field("state", &self.state)
            .field("payload", &self.payload)
            .finish()
    }
}

impl<K: Clone, V: Clone> Clone for IrMemEntry<K, V> {
    fn clone(&self) -> Self {
        Self {
            entry_type: self.entry_type,
            state: self.state,
            payload: self.payload.clone(),
        }
    }
}

/// IR operation state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IrState {
    /// Entry proposed but not yet decided by consensus.
    Tentative,
    /// Entry decided by consensus. The `u64` is the view number.
    Finalized(u64),
}

/// Inline payload for IR entries — the source of truth for transaction data.
///
/// Generic over `K` and `V` so that prepared transactions can be inspected,
/// indexed, and committed without a serialization round-trip while they live
/// in memory.  Serialization to bytes happens exactly once, at view seal
/// time (`seal_current_view`), via `UnifiedVlogSegment::serialize_payload`.
/// Deserialization from the VLog happens only for cross-view reads (rare).
///
/// Because `Commit` and `Abort` variants carry no K/V data, they are
/// unaffected by the type parameters.  Only `Prepare`, `QuorumRead`, and
/// `QuorumScan` hold typed fields.
pub enum IrPayloadInline<K, V> {
    /// CO::Prepare payload — the full transaction data for OCC validation.
    /// This is the SOLE carrier of write_set values; IO::Commit only has
    /// a PrepareRef back to this entry.
    Prepare {
        transaction_id: OccTransactionId,
        commit_ts: Timestamp,
        /// Read set: `(key, read_timestamp)` per read.
        read_set: Vec<(K, Timestamp)>,
        /// Write set: `(key, value)` per write. `None` = delete tombstone.
        /// MVCC UnifiedVlogPrepareValuePtr entries reference `write_set[index]`.
        write_set: Vec<(K, Option<V>)>,
        /// Scan set: `(start_key, end_key, timestamp)` per scan.
        scan_set: Vec<(K, K, Timestamp)>,
    },
    /// IO::Commit payload — commits a previously prepared transaction.
    Commit {
        transaction_id: OccTransactionId,
        commit_ts: Timestamp,
        prepare_ref: PrepareRef,
    },
    /// IO::Abort payload — aborts a prepared transaction.
    Abort {
        transaction_id: OccTransactionId,
        commit_ts: Option<Timestamp>,
    },
    /// IO::QuorumRead payload — RO transaction slow path quorum read.
    QuorumRead {
        key: K,
        timestamp: Timestamp,
    },
    /// IO::QuorumScan payload — RO transaction slow path quorum scan.
    QuorumScan {
        start_key: K,
        end_key: K,
        snapshot_ts: Timestamp,
    },
    /// CO::RaiseMinPrepareTime payload.
    RaiseMinPrepareTime {
        time: u64,
    },
}

impl<K: Debug, V: Debug> Debug for IrPayloadInline<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Prepare { transaction_id, commit_ts, read_set, write_set, scan_set } => {
                f.debug_struct("Prepare")
                    .field("transaction_id", transaction_id)
                    .field("commit_ts", commit_ts)
                    .field("read_set", read_set)
                    .field("write_set", write_set)
                    .field("scan_set", scan_set)
                    .finish()
            }
            Self::Commit { transaction_id, commit_ts, prepare_ref } => {
                f.debug_struct("Commit")
                    .field("transaction_id", transaction_id)
                    .field("commit_ts", commit_ts)
                    .field("prepare_ref", prepare_ref)
                    .finish()
            }
            Self::Abort { transaction_id, commit_ts } => {
                f.debug_struct("Abort")
                    .field("transaction_id", transaction_id)
                    .field("commit_ts", commit_ts)
                    .finish()
            }
            Self::QuorumRead { key, timestamp } => {
                f.debug_struct("QuorumRead")
                    .field("key", key)
                    .field("timestamp", timestamp)
                    .finish()
            }
            Self::QuorumScan { start_key, end_key, snapshot_ts } => {
                f.debug_struct("QuorumScan")
                    .field("start_key", start_key)
                    .field("end_key", end_key)
                    .field("snapshot_ts", snapshot_ts)
                    .finish()
            }
            Self::RaiseMinPrepareTime { time } => {
                f.debug_struct("RaiseMinPrepareTime")
                    .field("time", time)
                    .finish()
            }
        }
    }
}

impl<K: Clone, V: Clone> Clone for IrPayloadInline<K, V> {
    fn clone(&self) -> Self {
        match self {
            Self::Prepare { transaction_id, commit_ts, read_set, write_set, scan_set } => {
                Self::Prepare {
                    transaction_id: *transaction_id,
                    commit_ts: *commit_ts,
                    read_set: read_set.clone(),
                    write_set: write_set.clone(),
                    scan_set: scan_set.clone(),
                }
            }
            Self::Commit { transaction_id, commit_ts, prepare_ref } => {
                Self::Commit {
                    transaction_id: *transaction_id,
                    commit_ts: *commit_ts,
                    prepare_ref: prepare_ref.clone(),
                }
            }
            Self::Abort { transaction_id, commit_ts } => {
                Self::Abort {
                    transaction_id: *transaction_id,
                    commit_ts: *commit_ts,
                }
            }
            Self::QuorumRead { key, timestamp } => {
                Self::QuorumRead {
                    key: key.clone(),
                    timestamp: *timestamp,
                }
            }
            Self::QuorumScan { start_key, end_key, snapshot_ts } => {
                Self::QuorumScan {
                    start_key: start_key.clone(),
                    end_key: end_key.clone(),
                    snapshot_ts: *snapshot_ts,
                }
            }
            Self::RaiseMinPrepareTime { time } => {
                Self::RaiseMinPrepareTime { time: *time }
            }
        }
    }
}

/// Reference from an IO::Commit entry to its corresponding CO::Prepare entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PrepareRef {
    /// CO::Prepare is in the same view's overlay (common case).
    SameView(OpId),
    /// CO::Prepare is in a sealed view's VLog (rare cross-view case).
    CrossView {
        view: u64,
        vlog_ptr: UnifiedVlogPtr,
    },
}

/// IR SST entry: maps OpId → VLog location for the sealed IR base record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrSstEntry {
    pub entry_type: VlogEntryType,
    pub vlog_ptr: UnifiedVlogPtr,
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

/// In-memory IR record state: overlay (current view), base (sealed views),
/// and the prepare VLog index for cross-view commits.
pub(crate) struct IrRecord<K: Ord, V, IO: DiskIo> {
    active_vlog: UnifiedVlogSegment<IO>,
    sealed_vlog_segments: BTreeMap<u64, UnifiedVlogSegment<IO>>,
    current_view: u64,
    manifest: UnifiedManifest,

    /// Current view's IR entries (in-memory only).
    ///
    /// At seal time, finalized entries are serialized to the VLog and
    /// replaced by `IrSstEntry` records in `ir_base`.  The overlay is
    /// then cleared.  Typed K, V so that `Prepare` entries can be
    /// inspected without deserializing from the VLog.
    ir_overlay: BTreeMap<OpId, IrMemEntry<K, V>>,

    /// IR base: maps OpId → IrSstEntry for the sealed IR base record.
    /// In a full implementation this would be an on-disk IR SST.
    /// For now, we use an in-memory BTreeMap as a stepping stone.
    ir_base: BTreeMap<OpId, IrSstEntry>,

}

impl<K: Ord, V, IO: DiskIo> IrRecord<K, V, IO> {
    pub(crate) fn new(
        active_vlog: UnifiedVlogSegment<IO>,
        sealed_vlog_segments: BTreeMap<u64, UnifiedVlogSegment<IO>>,
        current_view: u64,
        manifest: UnifiedManifest,
    ) -> Self {
        Self {
            active_vlog,
            sealed_vlog_segments,
            current_view,
            manifest,
            ir_overlay: BTreeMap::new(),
            ir_base: BTreeMap::new(),
        }
    }

    /// Iterate over all IR overlay entries.
    pub(crate) fn ir_overlay_entries(&self) -> impl Iterator<Item = (&OpId, &IrMemEntry<K, V>)> {
        self.ir_overlay.iter()
    }

    pub(crate) fn current_view(&self) -> u64 {
        self.current_view
    }

    pub(crate) fn sealed_vlog_segments(&self) -> &BTreeMap<u64, UnifiedVlogSegment<IO>> {
        &self.sealed_vlog_segments
    }

    pub(crate) fn active_vlog_id(&self) -> u64 {
        self.active_vlog.id
    }

    pub(crate) fn active_vlog_write_offset(&self) -> u64 {
        self.active_vlog.write_offset()
    }

    pub(crate) fn active_vlog_views(&self) -> &[crate::unified::types::ViewRange] {
        &self.active_vlog.views
    }

    pub(crate) fn append_batch_to_active(
        &mut self,
        entries: &[(OpId, VlogEntryType, &IrPayloadInline<K, V>)],
    ) -> Result<Vec<UnifiedVlogPtr>, crate::mvcc::disk::error::StorageError>
    where
        K: serde::Serialize,
        V: serde::Serialize,
    {
        self.active_vlog.append_batch(entries)
    }

    pub(crate) fn set_current_view_for_install(&mut self, view: u64) {
        self.current_view = view;
        self.manifest.current_view = view;
    }

    pub(crate) fn active_or_sealed_segment(
        &self,
        segment_id: u64,
    ) -> Option<&UnifiedVlogSegment<IO>> {
        self.sealed_vlog_segments
            .get(&segment_id)
            .or(if self.active_vlog.id == segment_id {
                Some(&self.active_vlog)
            } else {
                None
            })
    }

    #[cfg(test)]
    pub(crate) fn read_payload(
        &self,
        ptr: &UnifiedVlogPtr,
    ) -> Result<(VlogEntryType, IrPayloadInline<K, V>), crate::mvcc::disk::error::StorageError>
    where
        K: serde::de::DeserializeOwned,
        V: serde::de::DeserializeOwned,
    {
        let segment = self
            .active_or_sealed_segment(ptr.segment_id)
            .ok_or_else(|| {
                crate::mvcc::disk::error::StorageError::Codec(format!(
                    "segment {} not found",
                    ptr.segment_id
                ))
            })?;

        let (_, ty, payload) = segment.read_entry::<K, V>(ptr)?;
        Ok((ty, payload))
    }

    pub(crate) fn clear_ir_base(&mut self) {
        self.ir_base.clear();
    }

    pub(crate) fn insert_ir_base_entry(&mut self, op_id: OpId, entry: IrSstEntry) {
        self.ir_base.insert(op_id, entry);
    }

    pub(crate) fn sealed_vlog_segment_values(
        &self,
    ) -> impl Iterator<Item = &UnifiedVlogSegment<IO>> {
        self.sealed_vlog_segments.values()
    }

    pub(crate) fn active_vlog_ref(&self) -> &UnifiedVlogSegment<IO> {
        &self.active_vlog
    }

    pub(crate) fn append_ir_entries_for_seal(
        &mut self,
        finalized_entries: &[(OpId, VlogEntryType, IrPayloadInline<K, V>)],
    ) -> Result<Vec<UnifiedVlogPtr>, crate::mvcc::disk::error::StorageError>
    where
        K: Clone + serde::Serialize,
        V: Clone + serde::Serialize,
    {
        if finalized_entries.is_empty() {
            return Ok(Vec::new());
        }
        let entry_refs: Vec<(OpId, VlogEntryType, &IrPayloadInline<K, V>)> = finalized_entries
            .iter()
            .map(|(op, et, p)| (*op, *et, p))
            .collect();
        self.active_vlog.append_batch(&entry_refs)
    }

    pub(crate) fn seal_active_view_and_rotate_if_needed(
        &mut self,
        base_dir: &std::path::Path,
        io_flags: crate::mvcc::disk::disk_io::OpenFlags,
        min_view_vlog_size: u64,
        finalized_count: usize,
    ) -> Result<(), crate::mvcc::disk::error::StorageError> {
        self.active_vlog.sync()?;
        self.active_vlog.finish_view(finalized_count as u32);

        let segment_size = self.active_vlog.write_offset();
        if segment_size >= min_view_vlog_size {
            let sealed_id = self.active_vlog.id;
            let sealed_path = self.active_vlog.path().clone();
            let sealed_views = self.active_vlog.views.clone();
            let sealed_size = self.active_vlog.write_offset();

            self.manifest.sealed_vlog_segments.push(crate::unified::types::VlogSegmentMeta {
                segment_id: sealed_id,
                path: sealed_path.clone(),
                views: sealed_views.clone(),
                total_size: sealed_size,
            });

            let old_active = std::mem::replace(&mut self.active_vlog, {
                let new_id = self.manifest.next_segment_id;
                self.manifest.next_segment_id += 1;
                let new_path = base_dir.join(format!("vlog_seg_{new_id:04}.dat"));
                UnifiedVlogSegment::<IO>::open(new_id, new_path, io_flags)?
            });

            self.sealed_vlog_segments.insert(
                sealed_id,
                UnifiedVlogSegment::<IO>::open_at(
                    sealed_id,
                    sealed_path,
                    sealed_size,
                    sealed_views,
                    io_flags,
                )?,
            );
            old_active.close();
        }

        self.current_view += 1;
        self.manifest.current_view = self.current_view;
        self.manifest.active_segment_id = self.active_vlog.id;
        self.manifest.active_write_offset = self.active_vlog.write_offset();
        self.manifest.save::<IO>(base_dir)?;
        self.active_vlog.start_view(self.current_view);

        Ok(())
    }

    /// Insert an IR entry into the overlay.
    pub(crate) fn insert_ir_entry(&mut self, op_id: OpId, entry: IrMemEntry<K, V>) {
        self.ir_overlay.insert(op_id, entry);
    }

    /// Look up an IR entry by OpId (overlay first, then base).
    pub(crate) fn ir_entry(&self, op_id: &OpId) -> Option<IrEntryRef<'_, K, V>> {
        if let Some(mem_entry) = self.ir_overlay.get(op_id) {
            Some(IrEntryRef::Overlay(mem_entry))
        } else {
            self.ir_base.get(op_id).map(IrEntryRef::Base)
        }
    }

    /// Look up an IR base SST entry by OpId.
    pub(crate) fn lookup_ir_base_entry(&self, op_id: OpId) -> Option<&IrSstEntry> {
        self.ir_base.get(&op_id)
    }

    /// Install a merged record as the new IR base from VLog pointers.
    ///
    /// Clears `ir_base` and `ir_overlay`, then populates `ir_base` from the
    /// given entries and their corresponding VLog pointers (written by the caller).
    pub(crate) fn install_base_from_ptrs(
        &mut self,
        entries: &[(OpId, IrMemEntry<K, V>)],
        ptrs: &[UnifiedVlogPtr],
    ) {
        self.ir_base.clear();
        for (i, (op_id, entry)) in entries.iter().enumerate() {
            self.ir_base.insert(
                *op_id,
                IrSstEntry {
                    entry_type: entry.entry_type,
                    vlog_ptr: ptrs[i],
                },
            );
        }
        self.ir_overlay.clear();
    }

    /// Apply VLog pointers to the IR base after a seal write.
    pub(crate) fn apply_sealed_ptrs(
        &mut self,
        finalized: &[(OpId, VlogEntryType, IrPayloadInline<K, V>)],
        vlog_ptrs: &[UnifiedVlogPtr],
    ) {
        for (i, (op_id, entry_type, _payload)) in finalized.iter().enumerate() {
            self.ir_base.insert(
                *op_id,
                IrSstEntry {
                    entry_type: *entry_type,
                    vlog_ptr: vlog_ptrs[i],
                },
            );
        }
    }

    /// Clear the overlay (called after seal or install).
    pub(crate) fn clear_overlay(&mut self) {
        self.ir_overlay.clear();
    }
}

impl<K: Ord + Clone, V: Clone, IO: DiskIo> IrRecord<K, V, IO> {
    /// Extract only finalized entries from the overlay (for leader merge simulation).
    pub(crate) fn extract_finalized_entries(&self) -> Vec<(OpId, IrMemEntry<K, V>)> {
        self.ir_overlay
            .iter()
            .filter(|(_, entry)| matches!(entry.state, IrState::Finalized(_)))
            .map(|(op_id, entry)| (*op_id, entry.clone()))
            .collect()
    }

    /// Collect finalized overlay entries for VLog serialization at seal time.
    ///
    /// Returns `(OpId, VlogEntryType, IrPayloadInline)` tuples ready for
    /// `UnifiedVlogSegment::append_batch`.  The caller writes them to the
    /// VLog and passes the resulting pointers to `apply_sealed_ptrs`.
    pub(crate) fn collect_finalized_for_seal(
        &self,
    ) -> Vec<(OpId, VlogEntryType, IrPayloadInline<K, V>)> {
        self.ir_overlay
            .iter()
            .filter(|(_, entry)| matches!(entry.state, IrState::Finalized(_)))
            .map(|(op_id, entry)| (*op_id, entry.entry_type, entry.payload.clone()))
            .collect()
    }
}
