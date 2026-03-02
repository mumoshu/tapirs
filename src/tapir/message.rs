use super::Timestamp;
use crate::{OccPrepareResult, OccSharedTransaction, OccTransactionId};
use serde::{Deserialize, Serialize};
use std::hash::Hash;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UO<K> {
    Get {
        /// Key to get the latest version of
        key: K,
    },
    /// Get a specific version at a given timestamp.
    GetAt {
        key: K,
        timestamp: Timestamp,
    },
    /// For backup coordinators.
    CheckPrepare {
        /// Id of transaction to check the preparedness of.
        transaction_id: OccTransactionId,
        /// Same as (any) known prepared timestamp.
        commit: Timestamp,
    },
    /// Range scan for transactional reads.
    Scan {
        start_key: K,
        end_key: K,
    },
    /// Range scan at a specific timestamp.
    ScanAt {
        start_key: K,
        end_key: K,
        timestamp: Timestamp,
    },
    /// For CDC-based resharding: read committed changes by view number.
    ScanChanges {
        from_view: u64,
    },
    /// Read-only transaction fast path: check if a replica has a "validated"
    /// version (read_ts >= snapshot_ts). If so, return the value without
    /// requiring a quorum.
    ReadValidated {
        key: K,
        timestamp: Timestamp,
    },
    /// Read-only transaction scan fast path: check if a replica has a covering
    /// range_read (read_ts >= snapshot_ts). If so, scan the MVCC store and
    /// return results without requiring a quorum.
    ScanValidated {
        start_key: K,
        end_key: K,
        snapshot_ts: Timestamp,
    },
    /// Query the read-protection watermark for a replacement shard during
    /// resharding (split, merge, compact).
    ///
    /// Returns `MinPrepareBaselineResult::Ok` with `max_read_time`: the
    /// highest timestamp seen across all read operations (QuorumRead,
    /// QuorumScan, and RW commit with reads).
    ///
    /// The caller should set `raise_min_prepare_time(max_read_time + 1)` on
    /// the new shard to subsume all historical read protections from the source.
    ///
    /// **Safety**: Only returns `Ok` when the shard is in `Decommissioning`
    /// phase (all IO::QuorumRead/QuorumScan blocked). Returns
    /// `NotDecommissioning` otherwise, because active reads would create a
    /// TOCTOU race — new read-protection entries could appear after the query.
    MinPrepareBaseline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UR<K, V> {
    /// To clients.
    Get(Option<V>, Timestamp),
    /// Result of a timestamp-specific get.
    GetAt(Option<V>, Timestamp),
    /// To backup coordinators.
    CheckPrepare(OccPrepareResult<Timestamp>),
    /// CDC scan results: leader record deltas for views >= from_view.
    ScanChanges {
        #[serde(bound(
            serialize = "K: Serialize, V: Serialize",
            deserialize = "K: Deserialize<'de>, V: Deserialize<'de>"
        ))]
        deltas: Vec<LeaderRecordDelta<K, V>>,
        /// The highest base_view for which this replica has a delta, or
        /// `None` if no deltas exist (no view changes have happened yet).
        ///
        /// A delta keyed by base_view=N contains changes that accumulated
        /// DURING view N (committed before the transition to view N+1).
        ///
        /// - `None` → no deltas to read; the replica has no CDC history.
        /// - `Some(N)` → deltas up through base_view=N have been returned.
        ///   Caller advances cursor with `from_view = N + 1`.
        ///
        /// This is `Option` rather than defaulting to 0 because base_view=0
        /// is a valid delta position (changes committed during view 0).
        /// Using `0` for "no deltas" would be ambiguous.
        effective_end_view: Option<u64>,
        /// Number of unresolved prepared transactions at this replica.
        /// Used by resharding drain to wait until all prepares resolve.
        pending_prepares: usize,
    },
    /// Range scan results.
    Scan(
        #[serde(bound(
            serialize = "K: Serialize, V: Serialize",
            deserialize = "K: Deserialize<'de>, V: Deserialize<'de>"
        ))]
        Vec<(K, Option<V>)>,
        Timestamp,
    ),
    /// Range scan results at a specific timestamp.
    ScanAt(
        #[serde(bound(
            serialize = "K: Serialize, V: Serialize",
            deserialize = "K: Deserialize<'de>, V: Deserialize<'de>"
        ))]
        Vec<(K, Option<V>)>,
        Timestamp,
    ),
    /// The requested key is outside this shard's current key range.
    OutOfRange,
    /// Read-only transaction fast path result: `Some((value, write_ts))` if the
    /// version is validated, `None` otherwise.
    ReadValidated(
        #[serde(bound(
            serialize = "V: Serialize",
            deserialize = "V: Deserialize<'de>"
        ))]
        Option<(Option<V>, Timestamp)>,
    ),
    /// Read-only transaction scan fast path result.
    /// `None` = no covering range_read at this replica (fall back to QuorumScan).
    /// `Some(vec)` = `(key, value, write_ts)` scan results from MVCC store.
    ScanValidated(
        #[serde(bound(
            serialize = "K: Serialize, V: Serialize",
            deserialize = "K: Deserialize<'de>, V: Deserialize<'de>"
        ))]
        Option<Vec<(K, Option<V>, Timestamp)>>,
    ),
    /// Result of `UO::MinPrepareBaseline`.
    MinPrepareBaseline(MinPrepareBaselineResult),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MinPrepareBaselineResult {
    Ok {
        max_read_time: u64,
    },
    NotDecommissioning,
}

/// A committed key-value change at a specific timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change<K, V> {
    pub transaction_id: OccTransactionId,
    pub key: K,
    pub value: Option<V>,
    pub timestamp: Timestamp,
}

/// Committed KV changes observed during a single view transition.
/// `from_view` is the view this replica was in before the transition.
/// `to_view` is the view it transitioned to.
/// For a replica that participated in every view change, from_view = to_view - 1.
/// For a replica that skipped views, from_view < to_view - 1 (coarse delta).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderRecordDelta<K, V> {
    pub from_view: u64,
    pub to_view: u64,
    #[serde(bound(
        serialize = "K: Serialize, V: Serialize",
        deserialize = "K: Deserialize<'de>, V: Deserialize<'de>"
    ))]
    pub changes: Vec<Change<K, V>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IO<K, V> {
    /// Commit a successfully prepared transaction.
    Commit {
        transaction_id: OccTransactionId,
        /// The full cross-shard transaction, containing read/write/scan sets for
        /// ALL participant shards (not just the receiving shard). Each receiving
        /// replica filters to shard-local keys in `OccStore::commit` via
        /// `shard_read_set()`/`shard_write_set()`.
        #[serde(bound(deserialize = "K: Ord + Deserialize<'de>, V: Deserialize<'de>"))]
        transaction: OccSharedTransaction<K, V, Timestamp>,
        /// Same as successfully prepared commit timestamp.
        commit: Timestamp,
    },
    /// Abort an unsuccessfully prepared transaction.
    ///
    /// Unlike TAPIR, tolerate `Abort` at any timestamp except
    /// that of a successful `Commit`.
    Abort {
        transaction_id: OccTransactionId,
        /// Same as unsuccessfully prepared commit timestamp for backup coordinators or `None`
        /// used by clients to abort at every timestamp.
        commit: Option<Timestamp>,
    },
    /// Read-only transaction slow path: quorum read via IR inconsistent op.
    /// Executed at FINALIZE time per the IR protocol. Sets read_ts to block
    /// future writes from overwriting the version.
    QuorumRead {
        #[serde(bound(deserialize = "K: Deserialize<'de>"))]
        key: K,
        timestamp: Timestamp,
    },
    /// Read-only transaction scan slow path: quorum scan via IR inconsistent op.
    /// At FINALIZE time, each replica scans the MVCC store and calls
    /// `commit_scan(start, end, snapshot_ts)` to record a range_read protecting
    /// the entire range from future writes at commit_ts < snapshot_ts.
    QuorumScan {
        #[serde(bound(deserialize = "K: Deserialize<'de>"))]
        start_key: K,
        #[serde(bound(deserialize = "K: Deserialize<'de>"))]
        end_key: K,
        snapshot_ts: Timestamp,
    },
}

impl<K: Ord, V: PartialEq> PartialEq for IO<K, V> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Commit {
                    transaction_id,
                    transaction,
                    commit,
                },
                Self::Commit {
                    transaction_id: other_transaction_id,
                    transaction: other_transaction,
                    commit: other_commit,
                },
            ) => {
                transaction_id == other_transaction_id
                    && transaction == other_transaction
                    && commit == other_commit
            }
            (
                Self::Abort {
                    transaction_id,
                    commit,
                },
                Self::Abort {
                    transaction_id: other_transaction_id,
                    commit: other_commit,
                },
            ) => transaction_id == other_transaction_id && commit == other_commit,
            (
                Self::QuorumRead { key, timestamp },
                Self::QuorumRead {
                    key: other_key,
                    timestamp: other_timestamp,
                },
            ) => key == other_key && timestamp == other_timestamp,
            (
                Self::QuorumScan {
                    start_key,
                    end_key,
                    snapshot_ts,
                },
                Self::QuorumScan {
                    start_key: other_start_key,
                    end_key: other_end_key,
                    snapshot_ts: other_snapshot_ts,
                },
            ) => {
                start_key == other_start_key
                    && end_key == other_end_key
                    && snapshot_ts == other_snapshot_ts
            }
            _ => false,
        }
    }
}

impl<K: Ord, V: Eq> Eq for IO<K, V> {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CO<K, V> {
    Prepare {
        /// Id of transaction to prepare.
        transaction_id: OccTransactionId,
        /// Transaction to prepare.
        #[serde(bound(deserialize = "K: Ord + Deserialize<'de>, V: Deserialize<'de>"))]
        transaction: OccSharedTransaction<K, V, Timestamp>,
        /// Proposed commit timestamp.
        commit: Timestamp,
    },
    RaiseMinPrepareTime {
        time: u64,
    },
}

impl<K: Ord, V: PartialEq> PartialEq for CO<K, V> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Prepare {
                    transaction_id,
                    transaction,
                    commit,
                },
                Self::Prepare {
                    transaction_id: other_transaction_id,
                    transaction: other_transaction,
                    commit: other_commit,
                },
            ) => {
                transaction_id == other_transaction_id
                    && transaction == other_transaction
                    && commit == other_commit
            }
            (
                Self::RaiseMinPrepareTime { time },
                Self::RaiseMinPrepareTime { time: other_time },
            ) => time == other_time,
            _ => false,
        }
    }
}

impl<K: Ord, V: Eq> Eq for CO<K, V> {}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum CR {
    Prepare(OccPrepareResult<Timestamp>),
    RaiseMinPrepareTime { time: u64 },
}

/// Inconsistent result type for read-only transaction operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IR<K, V> {
    QuorumRead(
        #[serde(bound(
            serialize = "V: Serialize",
            deserialize = "V: Deserialize<'de>"
        ))]
        Option<V>,
        Timestamp,
    ),
    /// QuorumScan results: `(key, value, write_ts)` for each key in range.
    QuorumScan(
        #[serde(bound(
            serialize = "K: Serialize, V: Serialize",
            deserialize = "K: Deserialize<'de>, V: Deserialize<'de>"
        ))]
        Vec<(K, Option<V>, Timestamp)>,
    ),
    OutOfRange,
    /// QuorumRead or QuorumScan conflicted with a prepared-but-uncommitted
    /// write at `commit_ts <= snapshot_ts` (see [`crate::occ::PrepareConflict`]).
    ///
    /// This is a tapirs extension to TAPIR, not an original TAPIR primitive.
    /// The original paper relies on piggybacking Finalize on the next Propose,
    /// but that is unreliable because IR Finalize is fire-and-forget. Instead,
    /// tapirs uses the OCC `prepared_writes` list to detect the conflict at
    /// the replica, and the ShardClient retries with exponential backoff until
    /// the prepare resolves (committed to MVCC or aborted).
    PrepareConflict,
}
