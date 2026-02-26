use std::collections::BTreeMap;

use super::types::{CloneError, SoloClusterManager};
use crate::backup::storage::BackupStorage;
use crate::backup::types::{ClusterMetadata, ShardBackupHistory, ShardDeltaInfo};

impl SoloClusterManager {
    /// Back up all shards by querying admin APIs directly.
    ///
    /// Queries each node's admin status to discover shard membership, creates
    /// ephemeral ShardClients, calls `scan_changes()` directly per shard, and
    /// writes delta files + `cluster.json`. Same pattern as `clone_shard_direct`
    /// but writes to files instead of shipping to a destination cluster.
    ///
    /// Supports incremental backup: reads existing `cluster.json` from
    /// `output_dir` to derive `last_backup_views` per shard.
    pub async fn backup_cluster_direct<S: BackupStorage>(
        &mut self,
        admin_addrs: &[String],
        storage: &S,
    ) -> Result<(), CloneError> {
        use crate::discovery::InMemoryShardDirectory;
        use crate::node::node_client::send_admin_request;
        use crate::{IrClientId, IrMembership, TapirReplica, TcpAddress, TcpTransport};
        use std::sync::Arc;

        storage
            .init()
            .await
            .map_err(CloneError::AdminError)?;

        // 1. Query all admin nodes to discover shard → listen_addrs.
        self.report_progress("backup-direct:discover-shards");
        let mut shard_addrs: BTreeMap<u32, Vec<TcpAddress>> = BTreeMap::new();
        for admin_addr in admin_addrs {
            let resp = send_admin_request(admin_addr, r#"{"command":"status"}"#)
                .await
                .map_err(|e| CloneError::AdminError(format!("query {admin_addr}: {e}")))?;
            if !resp.ok {
                return Err(CloneError::AdminError(format!(
                    "{admin_addr} status failed: {:?}",
                    resp.message
                )));
            }
            if let Some(shards) = resp.shards {
                for s in shards {
                    let addr: std::net::SocketAddr = s.listen_addr.parse().map_err(|e| {
                        CloneError::AdminError(format!(
                            "invalid listen_addr '{}': {e}",
                            s.listen_addr
                        ))
                    })?;
                    shard_addrs.entry(s.shard).or_default().push(TcpAddress(addr));
                }
            }
        }
        if shard_addrs.is_empty() {
            return Err(CloneError::AdminError(
                "no shards found on any node".to_string(),
            ));
        }

        // Deduplicate listen addrs per shard.
        for addrs in shard_addrs.values_mut() {
            addrs.sort_by_key(|a| a.0);
            addrs.dedup();
        }

        // 2. Load existing cluster.json for incremental backup.
        let mut existing: Option<ClusterMetadata> =
            if storage.exists("cluster.json").await.map_err(CloneError::AdminError)? {
                let data = storage.read_string("cluster.json").await.map_err(CloneError::AdminError)?;
                Some(serde_json::from_str(&data).map_err(|e| {
                    CloneError::AdminError(format!("parse cluster.json: {e}"))
                })?)
            } else {
                None
            };

        let mut last_backup_views: BTreeMap<u32, u64> = BTreeMap::new();
        if let Some(ref meta) = existing {
            for shard_hist in &meta.shards {
                if let Some(last_delta) = shard_hist.deltas.last() {
                    last_backup_views.insert(shard_hist.shard, last_delta.effective_end_view);
                }
            }
        }

        // 3. Create ephemeral transport.
        self.report_progress("backup-direct:create-transport");
        let ephemeral_addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
            let a = l.local_addr().unwrap();
            drop(l);
            TcpAddress(a)
        };
        let directory = Arc::new(InMemoryShardDirectory::new());
        let persist_dir = format!("/tmp/tapi_backup_{}", std::process::id());
        let transport: TcpTransport<TapirReplica<String, String>> =
            TcpTransport::with_directory(ephemeral_addr, persist_dir, directory);

        // 4. Per shard: create ShardClient, scan_changes, write delta file.
        self.report_progress("backup-direct:scan-shards");
        let mut shard_delta_counts: BTreeMap<u32, usize> = BTreeMap::new();
        if let Some(ref meta) = existing {
            for shard_hist in &meta.shards {
                shard_delta_counts.insert(shard_hist.shard, shard_hist.deltas.len());
            }
        }

        let mut shard_histories: Vec<ShardBackupHistory> = existing
            .take()
            .map(|m| m.shards)
            .unwrap_or_default();

        let mut sorted_shards: Vec<_> = shard_addrs.into_iter().collect();
        sorted_shards.sort_by_key(|(shard_num, _)| *shard_num);

        for (shard_num, addrs) in &sorted_shards {
            let shard = crate::ShardNumber(*shard_num);
            let membership = IrMembership::new(addrs.clone());
            transport.set_shard_addresses(shard, membership.clone());

            let client = crate::tapir::ShardClient::new(
                self.rng.fork(),
                IrClientId::new(&mut self.rng),
                shard,
                membership,
                transport.clone(),
            );

            let from_view = last_backup_views.get(shard_num).copied().unwrap_or(0);
            let result = client.scan_changes(from_view).await;

            // Skip shards with no deltas.
            if result.deltas.is_empty() && result.effective_end_view.is_none() {
                continue;
            }

            let seq = shard_delta_counts.get(shard_num).copied().unwrap_or(0);
            let file_name = format!("shard_{shard_num}_delta_{seq}.bin");

            let delta_bytes = bitcode::serialize(&result.deltas).map_err(|e| {
                CloneError::AdminError(format!("serialize deltas for shard {shard_num}: {e}"))
            })?;
            storage.write(&file_name, &delta_bytes).await.map_err(CloneError::AdminError)?;

            let effective_end_view = result.effective_end_view.unwrap_or(from_view);

            if from_view + 1 < effective_end_view {
                eprintln!(
                    "warning: shard {shard_num} delta spans views {from_view}..{effective_end_view} — membership/key_range snapshots may not reflect intermediate states"
                );
            }

            let replicas: Vec<String> = addrs.iter().map(|a| a.0.to_string()).collect();

            let delta_info = ShardDeltaInfo {
                file: file_name,
                from_view,
                effective_end_view,
                replicas_on_backup_taken: replicas,
                key_range_start_on_backup_taken: None,
                key_range_end_on_backup_taken: None,
            };

            if let Some(hist) = shard_histories.iter_mut().find(|h| h.shard == *shard_num) {
                hist.deltas.push(delta_info);
            } else {
                shard_histories.push(ShardBackupHistory {
                    shard: *shard_num,
                    deltas: vec![delta_info],
                });
            }
        }

        // 5. Write cluster.json.
        self.report_progress("backup-direct:write-metadata");
        let metadata = ClusterMetadata {
            shards: shard_histories,
            backup_timestamp: crate::backup::utc_now_iso8601(),
        };
        let json = serde_json::to_string_pretty(&metadata).map_err(|e| {
            CloneError::AdminError(format!("serialize cluster.json: {e}"))
        })?;
        storage.write("cluster.json", json.as_bytes()).await.map_err(CloneError::AdminError)?;

        self.report_progress("backup-direct:complete");
        Ok(())
    }
}
