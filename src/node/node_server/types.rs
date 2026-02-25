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
