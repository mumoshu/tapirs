use super::storage::BackupStorage;
use super::types::{BackupInfo, ClusterMetadata};

impl super::BackupManager {
    /// List available backups in a storage location.
    ///
    /// Scans the storage root and its immediate subdirectories for `cluster.json`
    /// files. Returns backup metadata sorted by timestamp (most recent first).
    /// This is a static operation — no shard manager connection needed.
    pub async fn list_backups<S: BackupStorage>(
        storage: &S,
    ) -> Result<Vec<BackupInfo>, String> {
        let mut backups = Vec::new();

        // Check the storage root itself.
        Self::try_load_backup(storage, &mut backups).await?;

        // Check immediate subdirectories.
        let subdirs = storage.list_subdirs().await?;
        for name in &subdirs {
            let sub = storage.sub(name);
            Self::try_load_backup(&sub, &mut backups).await?;
        }

        // Sort by timestamp descending (most recent first).
        backups.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(backups)
    }

    async fn try_load_backup<S: BackupStorage>(
        storage: &S,
        backups: &mut Vec<BackupInfo>,
    ) -> Result<(), String> {
        if !storage.exists("cluster.json").await? {
            return Ok(());
        }

        let data = storage.read_string("cluster.json").await?;
        let meta: ClusterMetadata = serde_json::from_str(&data)
            .map_err(|e| format!("parse cluster.json in {}: {e}", storage.display_path()))?;

        let delta_count: usize = meta.shards.iter().map(|s| s.deltas.len()).sum();
        backups.push(BackupInfo {
            path: storage.display_path(),
            timestamp: meta.backup_timestamp,
            shard_count: meta.shards.len(),
            delta_count,
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::backup::local::LocalBackupStorage;
    use crate::backup::types::{ClusterMetadata, ShardBackupHistory, ShardDeltaInfo};

    #[tokio::test]
    async fn list_backups_finds_cluster_json() {
        let tmp = std::env::temp_dir().join("tapi_test_list_backups");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        // Create a backup in a subdirectory.
        let sub = tmp.join("backup_001");
        std::fs::create_dir_all(&sub).unwrap();
        let meta = ClusterMetadata {
            shards: vec![ShardBackupHistory {
                shard: 0,
                deltas: vec![ShardDeltaInfo {
                    file: "shard_0_delta_0.bin".to_string(),
                    from_view: 0,
                    effective_end_view: 3,
                    replicas_on_backup_taken: vec!["127.0.0.1:8000".to_string()],
                    key_range_start_on_backup_taken: None,
                    key_range_end_on_backup_taken: None,
                }],
            }],
            backup_timestamp: "2025-01-15T10:00:00Z".to_string(),
        };
        let json = serde_json::to_string_pretty(&meta).unwrap();
        std::fs::write(sub.join("cluster.json"), json).unwrap();

        let storage = LocalBackupStorage::new(tmp.to_str().unwrap());
        let result = crate::backup::BackupManager::list_backups(&storage)
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].shard_count, 1);
        assert_eq!(result[0].delta_count, 1);
        assert_eq!(result[0].timestamp, "2025-01-15T10:00:00Z");

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
