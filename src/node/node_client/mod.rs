mod raw_exchange;
mod send_request;
pub mod backup_cluster;
pub mod restore_cluster;
pub mod types;

pub use raw_exchange::raw_admin_exchange;
pub use send_request::{admin_request, send_admin_request};
#[cfg(feature = "tls")]
pub use send_request::send_admin_request_tls;
pub use types::{ClusterMetadata, RestoreRequest};

use crate::node::ShardBackup;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct AdminStatusResponse {
    pub ok: bool,
    pub message: Option<String>,
    pub shards: Option<Vec<ShardInfoResponse>>,
    pub backup: Option<ShardBackup>,
}

#[derive(Deserialize)]
pub struct ShardInfoResponse {
    pub shard: u32,
    pub listen_addr: String,
}
