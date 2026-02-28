use crate::ir::OpId;
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use serde::{Deserialize, Serialize};

/// Physical pointer to an entry within the unified VLog.
///
/// Used by both MVCC index entries (via `UnifiedVlogPrepareValuePtr`) and IR SST
/// entries to locate payloads in sealed VLog segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnifiedVlogPtr {
    /// VLog segment file ID.
    pub segment_id: u64,
    /// Byte offset within the segment file where the entry starts.
    pub offset: u64,
    /// Total on-disk entry size in bytes (header + payload + CRC).
    pub length: u32,
}

/// Sub-pointer into a CO::Prepare VLog entry's write_set.
///
/// Instead of duplicating committed values in a separate MVCC vlog, each MVCC
/// index entry stores this pointer into the CO::Prepare's write_set. Reading
/// a committed value = read the CO::Prepare entry + extract write_set[index].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnifiedVlogPrepareValuePtr {
    /// Pointer to the CO::Prepare VLog entry containing the write_set.
    pub prepare_ptr: UnifiedVlogPtr,
    /// Index into the write_set entries within the Prepare payload.
    pub write_index: u16,
}

/// How a committed value is physically located — either in memory
/// (during the current view, before seal) or on disk (in a sealed VLog).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueLocation {
    /// Value is in memory in the `prepare_registry` (current view,
    /// not yet sealed). Converted to `OnDisk` at view seal time.
    InMemory {
        /// Transaction ID to look up in `prepare_registry`.
        txn_id: OccTransactionId,
        /// Index into the transaction's write_set.
        write_index: u16,
    },
    /// Value is in a sealed VLog segment (on disk). Used after
    /// view seal and in MVCC SST entries.
    OnDisk(UnifiedVlogPrepareValuePtr),
}

/// Entry in the MVCC index (memtable and SST).
///
/// Replaces `LsmEntry` from `src/mvcc/disk/memtable.rs` for the unified store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnifiedLsmEntry {
    /// Where to find the committed value bytes.
    /// `None` = delete tombstone or metadata-only entry (e.g., `commit_get`).
    pub value_ref: Option<ValueLocation>,
    /// OCC last-read timestamp for write-after-read conflict detection.
    pub last_read_ts: Option<u64>,
}

/// Entry type discriminator for unified VLog entries.
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

/// IR memtable entry. Holds data in memory for the current view's overlay.
#[derive(Debug, Clone)]
pub struct IrMemEntry {
    /// Which IR operation type this entry represents.
    pub entry_type: VlogEntryType,
    /// Whether this entry is Tentative or Finalized.
    pub state: IrState,
    // NOTE: No `modified_view` field. IrMemEntry only exists in the
    // overlay during the current view. modified_view would always
    // equal store.current_view().
    /// The full operation payload held inline in memory.
    pub payload: IrPayloadInline,
    // NOTE: No `vlog_ptr` field. VLog writes are deferred to view seal
    // time. At seal time, the overlay is discarded and replaced by
    // `IrSstEntry` records (which DO have `vlog_ptr`).
}

/// IR operation state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IrState {
    /// Entry proposed but not yet decided by consensus.
    Tentative,
    /// Entry decided by consensus. The `u64` is the view number.
    Finalized(u64),
}

/// Inline payload for IR entries.
///
/// All keys and values are opaque byte vectors — no encoding assumed.
#[derive(Debug, Clone)]
pub enum IrPayloadInline {
    /// CO::Prepare payload — the full transaction data for OCC validation.
    /// This is the SOLE carrier of write_set values; IO::Commit only has
    /// a PrepareRef back to this entry.
    Prepare {
        transaction_id: OccTransactionId,
        commit_ts: Timestamp,
        /// Read set: `(key_bytes, read_timestamp)` per read.
        read_set: Vec<(Vec<u8>, Timestamp)>,
        /// Write set: `(key_bytes, value_bytes)` per write.
        /// MVCC `UnifiedVlogPrepareValuePtr` entries reference `write_set[index]`.
        write_set: Vec<(Vec<u8>, Vec<u8>)>,
        /// Scan set: `(start_key, end_key, timestamp)` per scan.
        scan_set: Vec<(Vec<u8>, Vec<u8>, Timestamp)>,
        // NOTE: No `result` field — the OCC prepare result is only needed
        // by the IR consensus protocol (wire), not by VLog reads.
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
        key: Vec<u8>,
        timestamp: Timestamp,
    },
    /// IO::QuorumScan payload — RO transaction slow path quorum scan.
    QuorumScan {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        snapshot_ts: Timestamp,
    },
    /// CO::RaiseMinPrepareTime payload.
    RaiseMinPrepareTime {
        time: u64,
    },
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

/// Cached deserialized CO::Prepare payload stored in the LRU cache.
pub struct CachedPrepare {
    pub transaction_id: OccTransactionId,
    pub commit_ts: Timestamp,
    pub read_set: Vec<(Vec<u8>, Timestamp)>,
    pub write_set: Vec<(Vec<u8>, Vec<u8>)>,
    pub scan_set: Vec<(Vec<u8>, Vec<u8>, Timestamp)>,
}

/// Byte range and entry count for one view's entries within a VLog segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewRange {
    /// The IR view number whose entries are in this range.
    pub view: u64,
    /// Byte offset within the segment where this view's entries start.
    pub start_offset: u64,
    /// Byte offset where this view's entries end (exclusive).
    pub end_offset: u64,
    /// Number of VLog entries in this view's range.
    pub num_entries: u32,
}

/// Metadata for a sealed VLog segment stored in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VlogSegmentMeta {
    pub segment_id: u64,
    pub path: std::path::PathBuf,
    pub views: Vec<ViewRange>,
    pub total_size: u64,
}
