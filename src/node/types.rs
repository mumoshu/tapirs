use serde::Deserialize;

/// Configuration for adding a single replica to a node.
///
/// Used by [`Node::add_replica_no_join`] to specify shard number,
/// listen address, and initial membership.
#[derive(Deserialize)]
pub struct ReplicaConfig {
    pub shard: u32,
    pub listen_addr: String,
    pub membership: Vec<String>,
    /// Cluster type: "data" or "discovery". Stored in the manifest for
    /// S3 collision detection.
    #[serde(default = "default_cluster_type")]
    pub cluster_type: String,
}

fn default_cluster_type() -> String {
    "data".into()
}
