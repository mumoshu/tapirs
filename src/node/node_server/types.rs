use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct AdminRequest {
    pub command: String,
    #[serde(default)]
    pub shard: Option<u32>,
    #[serde(default)]
    pub listen_addr: Option<String>,
    #[serde(default)]
    pub storage: Option<String>,
    #[serde(default)]
    pub new_membership: Option<Vec<String>>,
    /// Static membership for add_replica. When provided, creates the replica
    /// with the specified membership directly (no shard-manager involvement).
    /// When absent, uses add_replica_join() which coordinates via shard-manager.
    #[serde(default)]
    pub membership: Option<Vec<String>>,
    /// S3 source for add_writable_clone_from_s3 and add_read_replica_from_s3.
    #[serde(default)]
    pub s3_source: Option<S3SourceConfig>,
    /// Refresh interval in seconds for add_read_replica_from_s3.
    #[serde(default)]
    pub refresh_interval_secs: Option<u64>,
    /// Cross-shard snapshot parameters for add_writable_clone_from_s3.
    /// The operator creates the snapshot and passes per-shard info.
    #[serde(default)]
    pub snapshot: Option<SnapshotParams>,
}

/// Snapshot parameters from a CrossShardSnapshot.
/// Ensures cross-shard consistency: each shard is cloned at a specific
/// manifest view and gets a ghost filter based on cutoff_ts/ceiling_ts.
#[derive(Deserialize)]
pub struct SnapshotParams {
    /// Global cutoff: min(max_committed_ts) across all shards.
    pub cutoff_ts: u64,
    /// Global ceiling: max(max_committed_ts) across all shards.
    pub ceiling_ts: u64,
    /// Manifest view to clone from for this shard.
    pub manifest_view: u64,
}

#[derive(Deserialize)]
pub struct S3SourceConfig {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct AdminResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub shards: Option<Vec<ShardInfo>>,
}

#[derive(Serialize, Deserialize)]
pub struct ShardInfo {
    pub shard: u32,
    pub listen_addr: String,
}
