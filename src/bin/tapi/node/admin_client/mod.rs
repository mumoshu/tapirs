mod backup_cluster;
mod raw_admin_exchange;
mod restore_cluster;
mod run;

pub(crate) use self::run::run;

use serde::{Deserialize, Serialize};
use tapirs::node::admin_client::send_admin_request;

/// Cluster metadata stored in cluster.json.
#[derive(Serialize, Deserialize)]
struct ClusterMetadata {
    shards: Vec<u32>,
    replicas_per_shard: std::collections::HashMap<u32, usize>,
}

#[derive(Serialize)]
struct RestoreRequest<'a> {
    command: &'static str,
    shard: u32,
    listen_addr: &'a str,
    backup: &'a super::ShardBackup,
    new_membership: &'a [String],
}

/// Send a parsed admin request, using TLS if configured.
async fn admin_request(
    addr: &str,
    request_json: &str,
    #[cfg(feature = "tls")] tls_connector: &Option<tapirs::tls::ReloadableTlsConnector>,
) -> Result<tapirs::node::admin_client::AdminStatusResponse, String> {
    #[cfg(feature = "tls")]
    if let Some(connector) = tls_connector {
        return tapirs::node::admin_client::send_admin_request_tls(
            addr,
            request_json,
            connector,
        )
        .await;
    }
    send_admin_request(addr, request_json).await
}
