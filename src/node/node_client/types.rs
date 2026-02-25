use crate::node::ShardBackup;
use serde::{Deserialize, Serialize};

/// Cluster metadata stored in cluster.json.
#[derive(Serialize, Deserialize)]
pub struct ClusterMetadata {
    pub shards: Vec<u32>,
    pub replicas_per_shard: std::collections::HashMap<u32, usize>,
}

#[derive(Serialize)]
pub struct RestoreRequest<'a> {
    pub command: &'static str,
    pub shard: u32,
    pub listen_addr: &'a str,
    pub backup: &'a ShardBackup,
    pub new_membership: &'a [String],
}
