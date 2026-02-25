// Re-export shim — all code moved to node_client module.
pub use super::node_client::{AdminStatusResponse, ShardInfoResponse, send_admin_request};
#[cfg(feature = "tls")]
pub use super::node_client::send_admin_request_tls;
