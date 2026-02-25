use super::types::{BackupInfo, ClusterMetadata};

impl super::BackupManager {
    /// List available backups in a directory.
    ///
    /// Scans `dir` and its immediate subdirectories for `cluster.json` files.
    /// Returns backup metadata sorted by timestamp (most recent first).
    /// This is a static operation — no shard manager connection needed.
    pub fn list_backups(dir: &str) -> Result<Vec<BackupInfo>, String> {
        let mut backups = Vec::new();

        // Check the directory itself.
        Self::try_load_backup(dir, &mut backups)?;

        // Check immediate subdirectories.
        let entries = std::fs::read_dir(dir)
            .map_err(|e| format!("read dir {dir}: {e}"))?;
        for entry in entries {
            let entry = entry.map_err(|e| format!("read dir entry: {e}"))?;
            if entry.file_type().is_ok_and(|ft| ft.is_dir()) {
                let path = entry.path();
                if let Some(path_str) = path.to_str() {
                    Self::try_load_backup(path_str, &mut backups)?;
                }
            }
        }

        // Sort by timestamp descending (most recent first).
        backups.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(backups)
    }

    fn try_load_backup(dir: &str, backups: &mut Vec<BackupInfo>) -> Result<(), String> {
        let cluster_json = format!("{dir}/cluster.json");
        if !std::path::Path::new(&cluster_json).exists() {
            return Ok(());
        }

        let data = std::fs::read_to_string(&cluster_json)
            .map_err(|e| format!("read {cluster_json}: {e}"))?;
        let meta: ClusterMetadata = serde_json::from_str(&data)
            .map_err(|e| format!("parse {cluster_json}: {e}"))?;

        let delta_count: usize = meta.shards.iter().map(|s| s.deltas.len()).sum();
        backups.push(BackupInfo {
            path: dir.to_string(),
            timestamp: meta.backup_timestamp,
            shard_count: meta.shards.len(),
            delta_count,
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::backup::types::{ClusterMetadata, ShardBackupHistory, ShardDeltaInfo};

    #[test]
    fn list_backups_finds_cluster_json() {
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

        let result = crate::backup::BackupManager::list_backups(tmp.to_str().unwrap()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].shard_count, 1);
        assert_eq!(result[0].delta_count, 1);
        assert_eq!(result[0].timestamp, "2025-01-15T10:00:00Z");

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
