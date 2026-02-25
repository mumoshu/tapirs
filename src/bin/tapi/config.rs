use serde::Deserialize;

#[derive(Default, Deserialize)]
pub struct NodeConfig {
    #[serde(default)]
    pub admin_listen_addr: Option<String>,
    #[serde(default)]
    pub metrics_listen_addr: Option<String>,
    #[serde(default)]
    pub persist_dir: Option<String>,
    #[serde(default)]
    pub shard_manager_url: Option<String>,
    #[serde(default)]
    pub replicas: Vec<ReplicaConfig>,
}

pub use tapirs::node::ReplicaConfig;

#[derive(Default, Deserialize)]
pub struct ClientConfig {
    #[serde(default)]
    pub shards: Vec<ShardConfig>,
}

#[derive(Deserialize)]
pub struct ShardConfig {
    pub id: u32,
    pub replicas: Vec<String>,
    #[serde(default)]
    pub key_range_start: Option<String>,
    #[serde(default)]
    pub key_range_end: Option<String>,
}

impl NodeConfig {
    pub fn from_file(path: &str) -> Self {
        let content = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("failed to read config {path}: {e}"));
        toml::from_str(&content)
            .unwrap_or_else(|e| panic!("failed to parse config {path}: {e}"))
    }
}

impl ClientConfig {
    pub fn from_file(path: &str) -> Self {
        let content = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("failed to read config {path}: {e}"));
        toml::from_str(&content)
            .unwrap_or_else(|e| panic!("failed to parse config {path}: {e}"))
    }
}
