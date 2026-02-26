pub mod local;
#[cfg(feature = "s3")]
pub mod s3backup;
pub mod storage;
mod backup_cluster;
mod list_backups;
mod restore;
pub mod types;

pub(crate) use backup_cluster::utc_now_iso8601;

use crate::sharding::shardmanager_client::HttpShardManagerClient;

/// Cross-cluster backup and restore orchestrator.
///
/// Unlike `ShardManager` (single-cluster shard lifecycle), `BackupManager`
/// coordinates between source and target clusters — backing up data from one
/// cluster and restoring to another.
///
/// Uses `HttpShardManagerClient` for data-plane operations (`scan_changes`,
/// `apply_changes`, `register`) and `send_admin_request` for creating
/// replicas on target nodes during restore.
pub struct BackupManager {
    shard_manager_client: HttpShardManagerClient,
    #[cfg(feature = "tls")]
    admin_tls_connector: Option<crate::tls::ReloadableTlsConnector>,
}

impl BackupManager {
    pub fn new(shard_manager_client: HttpShardManagerClient) -> Self {
        Self {
            shard_manager_client,
            #[cfg(feature = "tls")]
            admin_tls_connector: None,
        }
    }

    #[cfg(feature = "tls")]
    pub fn with_admin_tls(
        shard_manager_client: HttpShardManagerClient,
        admin_tls_connector: crate::tls::ReloadableTlsConnector,
    ) -> Self {
        Self {
            shard_manager_client,
            admin_tls_connector: Some(admin_tls_connector),
        }
    }
}
