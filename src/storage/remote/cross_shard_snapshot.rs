use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::backup::storage::BackupStorage;
use crate::storage::wisckeylsm::manifest::UnifiedManifest;

use super::ghost_filter::GhostFilter;
use super::manifest_store::RemoteManifestStore;

/// Cross-shard consistent snapshot.
///
/// `cutoff_ts = min(max_committed_ts)` across all shards — the consistent point.
/// `ceiling_ts = max(max_committed_ts)` across all shards — the upper bound.
/// Ghost filter hides `(cutoff_ts, ceiling_ts]` on ALL shards uniformly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossShardSnapshot {
    pub timestamp: String,
    /// Global cutoff: `min(max_committed_ts)` across all shards.
    /// Everything at or below is visible on all shards.
    pub cutoff_ts: u64,
    /// Global ceiling: `max(max_committed_ts)` across all shards.
    /// The ghost filter hides `(cutoff_ts, ceiling_ts]` on all shards.
    pub ceiling_ts: u64,
    /// Per-shard info: which manifest view to clone from.
    pub shards: BTreeMap<u32, ShardSnapshotInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSnapshotInfo {
    pub manifest_view: u64,
}

impl CrossShardSnapshot {
    /// Build the ghost filter for this snapshot.
    /// Returns None when cutoff_ts >= ceiling_ts (all shards at same ts,
    /// or single shard). An empty ghost filter is a no-op — applying it
    /// would hide nothing — so None is an optimization to skip the check.
    pub fn ghost_filter(&self) -> Option<GhostFilter> {
        let gf = GhostFilter {
            cutoff_ts: self.cutoff_ts,
            ceiling_ts: self.ceiling_ts,
        };
        if gf.is_empty() { None } else { Some(gf) }
    }
}

/// Create a cross-shard consistent snapshot from the latest manifest per shard.
///
/// Reads each shard's latest manifest, extracts `max_read_time` as the
/// shard's max_committed_ts, then computes:
/// - `cutoff_ts = min(max_committed_ts)` — the consistent point
/// - `ceiling_ts = max(max_committed_ts)` — the upper bound
pub async fn create_cross_shard_snapshot<S: BackupStorage>(
    manifest_store: &RemoteManifestStore<S>,
    shard_names: &[(u32, String)],
) -> Result<CrossShardSnapshot, String> {
    let mut shards = BTreeMap::new();
    let mut all_ts: Vec<u64> = Vec::new();
    for (shard_id, shard_name) in shard_names {
        let (view, bytes) = manifest_store.download_latest_manifest(shard_name).await?;
        let manifest: UnifiedManifest = bitcode::deserialize(&bytes)
            .map_err(|e| format!("deserialize manifest for {shard_name}: {e}"))?;
        // Skip shards with no committed data (max_read_time is None).
        // Only shards that have actually committed transactions contribute
        // to the cutoff/ceiling computation. A shard with no data has nothing
        // to ghost-filter.
        if let Some(ts) = manifest.max_read_time {
            all_ts.push(ts);
        }
        shards.insert(*shard_id, ShardSnapshotInfo {
            manifest_view: view,
        });
    }

    let cutoff_ts = all_ts.iter().copied().min().unwrap_or(0);
    let ceiling_ts = all_ts.iter().copied().max().unwrap_or(0);

    Ok(CrossShardSnapshot {
        timestamp: crate::backup::utc_now_iso8601(),
        cutoff_ts,
        ceiling_ts,
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
    fn ghost_filter_global() {
        let snapshot = CrossShardSnapshot {
            timestamp: "2026-01-15T10:30:00Z".into(),
            cutoff_ts: 150,
            ceiling_ts: 200,
            shards: BTreeMap::from([
                (0, ShardSnapshotInfo { manifest_view: 5 }),
                (1, ShardSnapshotInfo { manifest_view: 3 }),
            ]),
        };

        let gf = snapshot.ghost_filter().unwrap();
        assert_eq!(gf.cutoff_ts, 150);
        assert_eq!(gf.ceiling_ts, 200);
        assert!(gf.is_hidden(160));
        assert!(!gf.is_hidden(150));
        assert!(!gf.is_hidden(201));
    }

    #[test]
    fn no_ghost_filter_when_cutoff_equals_ceiling() {
        let snapshot = CrossShardSnapshot {
            timestamp: String::new(),
            cutoff_ts: 100,
            ceiling_ts: 100,
            shards: BTreeMap::from([
                (0, ShardSnapshotInfo { manifest_view: 1 }),
            ]),
        };
        assert!(snapshot.ghost_filter().is_none());
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
            ceiling_ts: 200,
            shards: BTreeMap::from([
                (0, ShardSnapshotInfo { manifest_view: 2 }),
            ]),
        };

        let name = save_snapshot(&storage, &snapshot).await.unwrap();
        let loaded = load_snapshot(&storage, &name).await.unwrap();

        assert_eq!(loaded.cutoff_ts, 100);
        assert_eq!(loaded.ceiling_ts, 200);
        assert_eq!(loaded.shards[&0].manifest_view, 2);
    }
}
