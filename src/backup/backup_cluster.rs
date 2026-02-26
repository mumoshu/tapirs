use std::collections::BTreeMap;

use super::storage::BackupStorage;
use super::types::{ClusterMetadata, ShardBackupHistory, ShardDeltaInfo};

/// Format the current UTC time as an ISO 8601 string (e.g. "2025-01-15T10:30:00Z").
///
/// Uses the Howard Hinnant civil_from_days algorithm to avoid a chrono dependency.
pub(crate) fn utc_now_iso8601() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let time_of_day = (secs % 86400) as u32;
    let h = time_of_day / 3600;
    let m = (time_of_day % 3600) / 60;
    let s = time_of_day % 60;

    // Civil date from days since 1970-01-01.
    // https://howardhinnant.github.io/date_algorithms.html#civil_from_days
    let mut z = (secs / 86400) as i64;
    z += 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };

    format!("{year:04}-{month:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

impl super::BackupManager {
    /// Perform a full or incremental backup of all active shards.
    ///
    /// Reads `cluster.json` from storage if it exists (incremental),
    /// otherwise starts from view 0 (full backup). Writes per-shard delta
    /// files and updates `cluster.json` with the new delta info.
    pub async fn backup_cluster<S: BackupStorage>(
        &self,
        storage: &S,
    ) -> Result<(), String> {
        storage.init().await?;

        // Load existing cluster.json for incremental backup.
        let mut existing: Option<ClusterMetadata> =
            if storage.exists("cluster.json").await? {
                let data = storage.read_string("cluster.json").await?;
                Some(
                    serde_json::from_str(&data)
                        .map_err(|e| format!("parse cluster.json: {e}"))?,
                )
            } else {
                None
            };

        // Derive last_backup_views from existing history.
        let mut last_backup_views: BTreeMap<u32, u64> = BTreeMap::new();
        if let Some(ref meta) = existing {
            for shard_hist in &meta.shards {
                if let Some(last_delta) = shard_hist.deltas.last() {
                    last_backup_views.insert(shard_hist.shard, last_delta.effective_end_view);
                }
            }
        }

        // Scan changes from shard manager.
        let response = self.shard_manager_client.scan_changes(&last_backup_views)?;

        // Calculate next delta sequence number per shard.
        let mut shard_delta_counts: BTreeMap<u32, usize> = BTreeMap::new();
        if let Some(ref meta) = existing {
            for shard_hist in &meta.shards {
                shard_delta_counts.insert(shard_hist.shard, shard_hist.deltas.len());
            }
        }

        // Build or update shard histories.
        let mut shard_histories: Vec<ShardBackupHistory> = existing
            .take()
            .map(|m| m.shards)
            .unwrap_or_default();

        for shard_data in &response.shards {
            // Skip shards with no deltas (no changes since last backup).
            if shard_data.deltas.is_empty() && shard_data.effective_end_view.is_none() {
                continue;
            }

            let seq = shard_delta_counts.get(&shard_data.shard).copied().unwrap_or(0);
            let file_name = format!("shard_{}_delta_{seq}.bin", shard_data.shard);

            // Serialize deltas as bitcode binary.
            let delta_bytes = bitcode::serialize(&shard_data.deltas)
                .map_err(|e| format!("serialize deltas for shard {}: {e}", shard_data.shard))?;
            storage.write(&file_name, &delta_bytes).await?;

            let from_view = last_backup_views.get(&shard_data.shard).copied().unwrap_or(0);
            let effective_end_view = shard_data.effective_end_view.unwrap_or(from_view);

            // Log warning if delta spans multiple views (membership/range may have changed).
            if from_view + 1 < effective_end_view {
                eprintln!(
                    "warning: shard {} delta spans views {}..{} — membership/key_range snapshots may not reflect intermediate states",
                    shard_data.shard, from_view, effective_end_view
                );
            }

            let delta_info = ShardDeltaInfo {
                file: file_name,
                from_view,
                effective_end_view,
                replicas_on_backup_taken: shard_data.replicas.clone(),
                key_range_start_on_backup_taken: shard_data.key_range_start.clone(),
                key_range_end_on_backup_taken: shard_data.key_range_end.clone(),
            };

            // Find or create shard history.
            if let Some(hist) = shard_histories.iter_mut().find(|h| h.shard == shard_data.shard) {
                hist.deltas.push(delta_info);
            } else {
                shard_histories.push(ShardBackupHistory {
                    shard: shard_data.shard,
                    deltas: vec![delta_info],
                });
            }
        }

        // Write updated cluster.json.
        let metadata = ClusterMetadata {
            shards: shard_histories,
            backup_timestamp: utc_now_iso8601(),
        };
        let json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| format!("serialize cluster.json: {e}"))?;
        storage.write("cluster.json", json.as_bytes()).await?;

        Ok(())
    }
}
