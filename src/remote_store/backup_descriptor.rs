use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::backup::storage::BackupStorage;

use super::manifest_store::RemoteManifestStore;

/// Per-shard backup descriptor: records which manifest version was
/// captured for each shard. Individually consistent (each shard is at
/// a valid view boundary) but NOT cross-shard consistent — use
/// `CrossShardSnapshot` for that.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupDescriptor {
    pub timestamp: String,
    /// shard_id → manifest view
    pub shards: BTreeMap<u32, u64>,
}

/// Create a backup descriptor from the latest manifest version per shard.
pub async fn create_backup<S: BackupStorage>(
    manifest_store: &RemoteManifestStore<S>,
    shard_names: &[(u32, String)],
) -> Result<BackupDescriptor, String> {
    let mut shards = BTreeMap::new();
    for (shard_id, shard_name) in shard_names {
        let versions = manifest_store.list_manifest_versions(shard_name).await?;
        let view = versions
            .last()
            .copied()
            .ok_or_else(|| format!("no manifests for shard {shard_name}"))?;
        shards.insert(*shard_id, view);
    }
    Ok(BackupDescriptor {
        timestamp: crate::backup::utc_now_iso8601(),
        shards,
    })
}

/// Save a backup descriptor to S3 under `backups/{timestamp}.json`.
pub async fn save_backup<S: BackupStorage>(
    storage: &S,
    descriptor: &BackupDescriptor,
) -> Result<String, String> {
    let name = format!("{}.json", descriptor.timestamp);
    let sub = storage.sub("backups");
    sub.init().await?;
    let json = serde_json::to_string_pretty(descriptor)
        .map_err(|e| format!("serialize backup descriptor: {e}"))?;
    sub.write(&name, json.as_bytes()).await?;
    Ok(name)
}

/// Load a backup descriptor from S3.
pub async fn load_backup<S: BackupStorage>(
    storage: &S,
    name: &str,
) -> Result<BackupDescriptor, String> {
    let sub = storage.sub("backups");
    let data = sub.read(name).await?;
    let text = String::from_utf8(data).map_err(|e| format!("invalid UTF-8: {e}"))?;
    serde_json::from_str(&text).map_err(|e| format!("parse backup descriptor: {e}"))
}

/// List available backup descriptors.
pub async fn list_backups<S: BackupStorage>(storage: &S) -> Result<Vec<String>, String> {
    let sub = storage.sub("backups");
    sub.list_subdirs().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::local::LocalBackupStorage;

    #[tokio::test(flavor = "current_thread")]
    async fn roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let storage = LocalBackupStorage::new(dir.path().to_str().unwrap());
        storage.init().await.unwrap();

        let desc = BackupDescriptor {
            timestamp: "2026-01-15T10:30:00Z".into(),
            shards: BTreeMap::from([(0, 5), (1, 3)]),
        };

        let name = save_backup(&storage, &desc).await.unwrap();
        let loaded = load_backup(&storage, &name).await.unwrap();

        assert_eq!(loaded.shards[&0], 5);
        assert_eq!(loaded.shards[&1], 3);
    }
}
