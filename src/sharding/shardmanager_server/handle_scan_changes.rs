use std::collections::BTreeMap;

use crate::discovery::{RemoteShardDirectory, membership_to_strings};
use crate::sharding::shardmanager::scan_changes_types::{ScanChangesResponse, ShardScanChangesData};
use crate::TcpAddress;
use serde::Deserialize;
use super::ShardManagerState;

#[derive(Deserialize)]
struct ScanChangesRequest {
    #[serde(default)]
    last_backup_views: BTreeMap<u32, u64>,
}

pub(crate) async fn handle_scan_changes<RD: RemoteShardDirectory<TcpAddress, String>>(
    state: &ShardManagerState<RD>,
    body: &[u8],
) -> (u16, Vec<u8>) {
    // Parse JSON request body (empty body = full backup from view 0).
    let req: ScanChangesRequest = if body.is_empty() {
        ScanChangesRequest { last_backup_views: BTreeMap::new() }
    } else {
        match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => {
                let err = format!(r#"{{"error":"invalid JSON: {e}"}}"#);
                return (400, err.into_bytes());
            }
        }
    };

    // List all active shards (no lock needed).
    let shard_list = match state.remote.strong_all_active_shard_view_memberships().await {
        Ok(list) => list,
        Err(e) => {
            let err = format!(r#"{{"error":"discovery error: {e:?}"}}"#);
            return (500, err.into_bytes());
        }
    };

    let mut shards_data = Vec::new();
    for (shard, membership, _view) in &shard_list {
        // Backup requires multi-replica shards (f >= 1, i.e., 3+ replicas).
        if membership.len() < 3 {
            let err = format!(
                r#"{{"error":"backup requires multi-replica shards (3+), shard {} has {} replicas"}}"#,
                shard.0,
                membership.len()
            );
            return (400, err.into_bytes());
        }

        let from_view = req.last_backup_views.get(&shard.0).copied().unwrap_or(0);

        // Query key_range from discovery.
        let key_range = match state.remote.strong_get_shard(*shard).await {
            Ok(Some(record)) => record.key_range,
            _ => None,
        };

        // Create ShardClient (brief lock for make_shard_client).
        let client = {
            state.manager.lock().await.make_shard_client(*shard, membership.clone())
        };

        // Run scan_changes outside the lock.
        let result = client.scan_changes(from_view).await;

        let replicas = membership_to_strings(membership);
        let (key_range_start, key_range_end) = match key_range {
            Some(kr) => (kr.start, kr.end),
            None => (None, None),
        };

        shards_data.push(ShardScanChangesData {
            shard: shard.0,
            replicas,
            key_range_start,
            key_range_end,
            effective_end_view: result.effective_end_view,
            deltas: result.deltas,
        });
    }

    let response = ScanChangesResponse { shards: shards_data };
    match bitcode::serialize(&response) {
        Ok(bytes) => (200, bytes),
        Err(e) => {
            let err = format!(r#"{{"error":"serialization failed: {e}"}}"#);
            (500, err.into_bytes())
        }
    }
}
