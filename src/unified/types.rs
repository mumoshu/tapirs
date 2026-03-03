use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use serde::{Deserialize, Serialize};

pub use super::ir::record::{
    IrEntryRef, IrMemEntry, IrPayloadInline, IrSstEntry, IrState, PrepareRef, VlogEntryType,
};

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

/// Sub-pointer into a TAPIR committed-transaction VLog entry's write_set.
///
/// Instead of duplicating committed values in a separate MVCC vlog, each MVCC
/// index entry stores this pointer into the committed transaction's write_set.
/// Reading a committed value = read the transaction entry + extract
/// `write_set[index]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnifiedVlogPrepareValuePtr {
    /// Pointer to the committed transaction VLog entry containing the write_set.
    pub txn_ptr: UnifiedVlogPtr,
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

/// Deserialized committed-transaction payload with typed keys and values.
///
/// Shared via `Arc` between two lookup paths:
///
/// - **`prepare_registry`** — current view's in-memory prepares. Populated
///   by `register_prepare()`, read by `resolve_in_memory()`.  Cleared at
///   seal time because the data moves to the VLog.
///
/// - **`prepare_cache`** — LRU cache for committed transactions deserialized from sealed
///   VLog segments.  Populated on first `resolve_on_disk()` miss.  Avoids
///   repeated VLog reads for hot cross-view transactions.
///
/// The `write_set` uses `Option<V>` where `None` represents a delete
/// tombstone (matching OCC convention).  MVCC index entries reference
/// individual write_set items via `write_index`.
pub struct CachedPrepare<K, V> {
    pub transaction_id: OccTransactionId,
    /// Prepare-time (proposed) commit timestamp.
    ///
    /// This is the timestamp the client proposed at prepare time, NOT
    /// necessarily the final commit timestamp.  In TAPIR, replicas may
    /// return `Retry { proposed }` with a higher timestamp, and the
    /// coordinator picks the maximum as the final commit timestamp.
    /// The final timestamp is passed separately to `commit_prepared()`.
    pub commit_ts: Timestamp,
    pub read_set: Vec<(K, Timestamp)>,
    pub write_set: Vec<(K, Option<V>)>,
    pub scan_set: Vec<(K, K, Timestamp)>,
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
