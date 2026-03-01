use crate::ir::OpId;
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

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
        /// MVCC `UnifiedVlogPrepareValuePtr` entries reference `write_set[index]`.
        write_set: Vec<(K, Option<V>)>,
        /// Scan set: `(start_key, end_key, timestamp)` per scan.
        scan_set: Vec<(K, K, Timestamp)>,
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

/// Deserialized CO::Prepare payload with typed keys and values.
///
/// Shared via `Arc` between two lookup paths:
///
/// - **`prepare_registry`** — current view's in-memory prepares.  Populated
///   by `register_prepare()`, read by `resolve_in_memory()`.  Cleared at
///   seal time because the data moves to the VLog.
///
/// - **`prepare_cache`** — LRU cache for prepares deserialized from sealed
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
