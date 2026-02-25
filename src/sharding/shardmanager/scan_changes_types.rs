use crate::tapir::LeaderRecordDelta;
use serde::{Deserialize, Serialize};

/// Aggregated response from the shard manager's `/v1/scan-changes` endpoint.
///
/// Contains CDC delta data for all active shards in the cluster, serialized
/// as bitcode for efficient transfer. Used by `BackupManager` for both full
/// and incremental backup.
#[derive(Serialize, Deserialize)]
pub struct ScanChangesResponse {
    pub shards: Vec<ShardScanChangesData>,
}

/// Per-shard CDC scan-changes data included in [`ScanChangesResponse`].
///
/// Fields snapshot the shard's state at the time the scan was performed.
/// `replicas` and `key_range_*` are point-in-time values — a shard may have
/// been split/merged/joined/left between `from_view` and `effective_end_view`.
#[derive(Serialize, Deserialize)]
pub struct ShardScanChangesData {
    pub shard: u32,
    pub replicas: Vec<String>,
    pub key_range_start: Option<String>,
    pub key_range_end: Option<String>,
    pub effective_end_view: Option<u64>,
    pub deltas: Vec<LeaderRecordDelta<String, String>>,
}
