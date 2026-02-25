use serde::{Deserialize, Serialize};

/// Cluster metadata stored in `cluster.json` within the backup directory.
///
/// Contains per-shard backup history across multiple backup sessions.
/// Updated incrementally — each `backup_cluster()` call appends a new
/// delta to the appropriate shard's history.
#[derive(Serialize, Deserialize)]
pub struct ClusterMetadata {
    pub shards: Vec<ShardBackupHistory>,
    pub backup_timestamp: String,
}

/// Per-shard backup history: an ordered list of delta files produced
/// across one or more backup sessions.
#[derive(Serialize, Deserialize)]
pub struct ShardBackupHistory {
    pub shard: u32,
    pub deltas: Vec<ShardDeltaInfo>,
}

/// Metadata for a single delta file produced by one `scan_changes()` call.
///
/// The `_on_backup_taken` fields are point-in-time snapshots of the shard's
/// membership and key range at the time the backup was performed. These may
/// not reflect intermediate states if the shard was split/merged/joined
/// within the delta's view range.
#[derive(Serialize, Deserialize)]
pub struct ShardDeltaInfo {
    pub file: String,
    pub from_view: u64,
    pub effective_end_view: u64,
    pub replicas_on_backup_taken: Vec<String>,
    pub key_range_start_on_backup_taken: Option<String>,
    pub key_range_end_on_backup_taken: Option<String>,
}

/// Summary information about a backup directory, used by `list_backups`.
pub struct BackupInfo {
    pub path: String,
    pub timestamp: String,
    pub shard_count: usize,
    pub delta_count: usize,
}
