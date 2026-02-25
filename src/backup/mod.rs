mod backup_cluster;
mod list_backups;
pub mod types;

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
}

impl BackupManager {
    pub fn new(shard_manager_client: HttpShardManagerClient) -> Self {
        Self {
            shard_manager_client,
        }
    }
}
