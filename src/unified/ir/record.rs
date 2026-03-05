use crate::ir::OpId;
use crate::mvcc::disk::disk_io::DiskIo;
use crate::mvcc::disk::error::StorageError;
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use crate::unified::wisckeylsm::lsm::VlogLsm;
use crate::unified::wisckeylsm::manifest::UnifiedManifest;
use crate::unified::wisckeylsm::types::VlogPtr;
use crate::unified::wisckeylsm::vlog::VlogSegment;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::PathBuf;

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
/// via VlogLsm's seal_view() closure.
#[derive(Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: Deserialize<'de>, V: Deserialize<'de>"
))]
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

impl IrState {
    pub fn is_finalized(&self) -> bool {
        matches!(self, Self::Finalized(_))
    }
}

/// Inline payload for IR entries — the source of truth for transaction data.
///
/// Generic over `K` and `V` so that prepared transactions can be inspected,
/// indexed, and committed without a serialization round-trip while they live
/// in memory.  Serialization to bytes happens exactly once, at view seal
/// time (`seal_current_view`), via VlogLsm's seal_view() closure.
/// Deserialization from the VLog happens only for cross-view reads (rare).
///
/// Because `Commit` and `Abort` variants carry no K/V data, they are
/// unaffected by the type parameters.  Only `Prepare`, `QuorumRead`, and
/// `QuorumScan` hold typed fields.
#[derive(Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: Deserialize<'de>, V: Deserialize<'de>"
))]
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
        /// MVCC LsmEntry entries reference `write_set[index]`.
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
        vlog_ptr: VlogPtr,
    },
}

/// In-memory IR record state backed by VlogLsm.
///
/// All entries (tentative + finalized) go into the VlogLsm memtable via put().
/// At seal time, seal_view() flushes finalized entries to vlog+index and
/// discards tentative entries. Recovery rebuilds the VlogLsm index from
/// vlog segments.
pub(crate) struct IrRecord<K: Ord, V, IO: DiskIo> {
    lsm: VlogLsm<OpId, IrMemEntry<K, V>, IO>,
    current_view: u64,
    manifest: UnifiedManifest,
    base_dir: PathBuf,
}

impl<K: Ord, V, IO: DiskIo> IrRecord<K, V, IO> {
    pub(crate) fn new(
        lsm: VlogLsm<OpId, IrMemEntry<K, V>, IO>,
        current_view: u64,
        manifest: UnifiedManifest,
        base_dir: PathBuf,
    ) -> Self {
        Self {
            lsm,
            current_view,
            manifest,
            base_dir,
        }
    }

    pub(crate) fn current_view(&self) -> u64 {
        self.current_view
    }

    /// Insert an IR entry into the memtable.
    pub(crate) fn insert_ir_entry(&mut self, op_id: OpId, entry: IrMemEntry<K, V>) {
        self.lsm.put(op_id, entry);
    }

    /// Seal: flush finalized entries to vlog via closure, discard tentative,
    /// update manifest, increment view.
    pub(crate) fn seal_current_view(
        &mut self,
        min_vlog_size: u64,
    ) -> Result<(), StorageError>
    where
        K: Serialize + serde::de::DeserializeOwned + Clone,
        V: Serialize,
    {
        let sealed = self.lsm.seal_view(min_vlog_size, |op_id, entry| {
            if entry.state.is_finalized() {
                Some((entry.entry_type as u8, op_id.client_id.0, op_id.number))
            } else {
                None // tentative → discarded
            }
        })?;
        if let Some(meta) = sealed {
            self.manifest.ir.sealed_vlog_segments.push(meta);
        }
        self.current_view += 1;
        self.manifest.current_view = self.current_view;
        self.manifest.ir.active_segment_id = self.lsm.active_vlog_id();
        self.manifest.ir.active_write_offset = self.lsm.active_write_offset();
        self.manifest.ir.next_segment_id = self.lsm.next_segment_id();
        self.manifest.save::<IO>(&self.base_dir)?;
        self.lsm.start_view(self.current_view);
        Ok(())
    }

    /// Look up a vlog segment by ID (active or sealed).
    pub(crate) fn segment_ref(&self, id: u64) -> Option<&VlogSegment<IO>> {
        self.lsm.segment_ref(id)
    }

    /// Reference to sealed vlog segments.
    pub(crate) fn sealed_segments_ref(&self) -> &BTreeMap<u64, VlogSegment<IO>> {
        self.lsm.sealed_segments_ref()
    }

    /// Reference to the active vlog segment.
    pub(crate) fn active_vlog_ref(&self) -> &VlogSegment<IO> {
        self.lsm.active_vlog_ref()
    }
}
