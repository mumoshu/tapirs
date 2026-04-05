use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::backup::storage::BackupStorage;
use crate::unified::wisckeylsm::manifest::UnifiedManifest;

use super::ghost_filter::GhostFilter;
use super::manifest_store::RemoteManifestStore;

/// Cross-shard consistent snapshot with per-shard ghost filter parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossShardSnapshot {
    pub timestamp: String,
    /// Global cutoff: `min(ceiling_ts)` across all shards.
    pub cutoff_ts: u64,
    /// Per-shard info: which manifest view to restore from, and this
    /// shard's ceiling_ts (its max_committed_ts at backup time).
    pub shards: BTreeMap<u32, ShardSnapshotInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSnapshotInfo {
    pub manifest_view: u64,
    pub ceiling_ts: u64,
}

impl CrossShardSnapshot {
    /// Build a GhostFilter for a specific shard from this snapshot.
    /// Returns None if the shard's ceiling equals the cutoff (no hidden range).
    pub fn ghost_filter_for(&self, shard_id: u32) -> Option<GhostFilter> {
        let info = self.shards.get(&shard_id)?;
        let gf = GhostFilter {
            cutoff_ts: self.cutoff_ts,
            ceiling_ts: info.ceiling_ts,
        };
        if gf.is_empty() { None } else { Some(gf) }
    }
}

/// Create a cross-shard consistent snapshot from the latest manifest per shard.
///
/// Reads each shard's latest manifest, extracts `max_read_time` as the
/// shard's ceiling_ts, then computes `cutoff_ts = min(ceiling_ts)`.
pub async fn create_cross_shard_snapshot<S: BackupStorage>(
    manifest_store: &RemoteManifestStore<S>,
    shard_names: &[(u32, String)],
) -> Result<CrossShardSnapshot, String> {
    let mut shards = BTreeMap::new();
    for (shard_id, shard_name) in shard_names {
        let (view, bytes) = manifest_store.download_latest_manifest(shard_name).await?;
        let manifest: UnifiedManifest = bitcode::deserialize(&bytes)
            .map_err(|e| format!("deserialize manifest for {shard_name}: {e}"))?;
        let ceiling_ts = manifest
            .max_read_time
            .unwrap_or(0);
        shards.insert(*shard_id, ShardSnapshotInfo {
            manifest_view: view,
            ceiling_ts,
        });
    }

    let cutoff_ts = shards
        .values()
        .map(|info| info.ceiling_ts)
        .min()
        .unwrap_or(0);

    Ok(CrossShardSnapshot {
        timestamp: crate::backup::utc_now_iso8601(),
        cutoff_ts,
        shards,
    })
}

/// Save a cross-shard snapshot to S3.
pub async fn save_snapshot<S: BackupStorage>(
    storage: &S,
    snapshot: &CrossShardSnapshot,
) -> Result<String, String> {
    let name = format!("{}.json", snapshot.timestamp);
    let sub = storage.sub("snapshots");
    sub.init().await?;
    let json = serde_json::to_string_pretty(snapshot)
        .map_err(|e| format!("serialize snapshot: {e}"))?;
    sub.write(&name, json.as_bytes()).await?;
    Ok(name)
}

/// Load a cross-shard snapshot from S3.
pub async fn load_snapshot<S: BackupStorage>(
    storage: &S,
    name: &str,
) -> Result<CrossShardSnapshot, String> {
    let sub = storage.sub("snapshots");
    let data = sub.read(name).await?;
    let text = String::from_utf8(data).map_err(|e| format!("invalid UTF-8: {e}"))?;
    serde_json::from_str(&text).map_err(|e| format!("parse snapshot: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ghost_filter_for_shard() {
        let snapshot = CrossShardSnapshot {
            timestamp: "2026-01-15T10:30:00Z".into(),
            cutoff_ts: 150,
            shards: BTreeMap::from([
                (0, ShardSnapshotInfo { manifest_view: 5, ceiling_ts: 200 }),
                (1, ShardSnapshotInfo { manifest_view: 3, ceiling_ts: 150 }),
            ]),
        };

        // Shard 0: ceiling > cutoff → has ghost filter.
        let gf0 = snapshot.ghost_filter_for(0).unwrap();
        assert_eq!(gf0.cutoff_ts, 150);
        assert_eq!(gf0.ceiling_ts, 200);
        assert!(gf0.is_hidden(160));
        assert!(!gf0.is_hidden(150));

        // Shard 1: ceiling == cutoff → no ghost filter needed.
        assert!(snapshot.ghost_filter_for(1).is_none());

        // Unknown shard → None.
        assert!(snapshot.ghost_filter_for(99).is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::backup::local::LocalBackupStorage::new(
            dir.path().to_str().unwrap(),
        );
        storage.init().await.unwrap();

        let snapshot = CrossShardSnapshot {
            timestamp: "2026-01-15T10:30:00Z".into(),
            cutoff_ts: 100,
            shards: BTreeMap::from([
                (0, ShardSnapshotInfo { manifest_view: 2, ceiling_ts: 200 }),
            ]),
        };

        let name = save_snapshot(&storage, &snapshot).await.unwrap();
        let loaded = load_snapshot(&storage, &name).await.unwrap();

        assert_eq!(loaded.cutoff_ts, 100);
        assert_eq!(loaded.shards[&0].ceiling_ts, 200);
    }
}
