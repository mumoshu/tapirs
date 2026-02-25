use super::types::{CloneError, SoloClusterManager};
use crate::backup::types::ClusterMetadata;

impl SoloClusterManager {
    /// Restore a cluster from backup files by communicating directly with nodes.
    ///
    /// Two-phase process (no Phase 3 — solo clusters have no discovery):
    /// 1. **Prepare**: Create empty replicas on target nodes via admin API.
    /// 2. **Apply**: Ship delta data to each shard via ephemeral ShardClients.
    ///
    /// Same pattern as `clone_shard_direct` for transport creation, combined
    /// with the restore logic from `BackupManager::restore_cluster`.
    pub async fn restore_cluster_direct(
        &mut self,
        admin_addrs: &[String],
        backup_dir: &str,
    ) -> Result<(), CloneError> {
        use crate::discovery::InMemoryShardDirectory;
        use crate::node::node_client::send_admin_request;
        use crate::sharding::shardmanager::cdc::ship_changes;
        use crate::tapir::LeaderRecordDelta;
        use crate::{IrClientId, IrMembership, TapirReplica, TcpAddress, TcpTransport};
        use std::sync::Arc;

        // Read cluster.json.
        let cluster_json_path = format!("{backup_dir}/cluster.json");
        let data = std::fs::read_to_string(&cluster_json_path)
            .map_err(|e| CloneError::AdminError(format!("read cluster.json: {e}")))?;
        let meta: ClusterMetadata = serde_json::from_str(&data)
            .map_err(|e| CloneError::AdminError(format!("parse cluster.json: {e}")))?;

        if meta.shards.is_empty() {
            return Err(CloneError::AdminError("no shards in backup".to_string()));
        }

        // Phase 1: Prepare — create empty replicas on target nodes.
        self.report_progress("restore-direct:create-replicas");
        for shard_hist in &meta.shards {
            let last_delta = shard_hist.deltas.last().ok_or_else(|| {
                CloneError::AdminError(format!("shard {} has no deltas", shard_hist.shard))
            })?;

            let replicas = &last_delta.replicas_on_backup_taken;
            if admin_addrs.len() < replicas.len() {
                return Err(CloneError::AdminError(format!(
                    "shard {} has {} replicas but only {} admin addrs provided",
                    shard_hist.shard,
                    replicas.len(),
                    admin_addrs.len()
                )));
            }

            let membership_json: Vec<String> = replicas.clone();
            for (i, listen_addr) in replicas.iter().enumerate() {
                let admin_addr = &admin_addrs[i];
                let request = serde_json::json!({
                    "command": "add_replica",
                    "shard": shard_hist.shard,
                    "listen_addr": listen_addr,
                    "membership": membership_json,
                });

                let resp = send_admin_request(admin_addr, &request.to_string())
                    .await
                    .map_err(|e| {
                        CloneError::AdminError(format!(
                            "create replica on {admin_addr} for shard {}: {e}",
                            shard_hist.shard
                        ))
                    })?;
                if !resp.ok {
                    return Err(CloneError::AdminError(format!(
                        "create replica on {admin_addr} for shard {}: {}",
                        shard_hist.shard,
                        resp.message.as_deref().unwrap_or("unknown error")
                    )));
                }
            }
        }

        // Phase 2: Apply — ship delta data via ephemeral ShardClients.
        self.report_progress("restore-direct:apply-deltas");
        let ephemeral_addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
            let a = l.local_addr().unwrap();
            drop(l);
            TcpAddress(a)
        };
        let directory = Arc::new(InMemoryShardDirectory::new());
        let persist_dir = format!("/tmp/tapi_restore_{}", std::process::id());
        let transport: TcpTransport<TapirReplica<String, String>> =
            TcpTransport::with_directory(ephemeral_addr, persist_dir, directory);

        for shard_hist in &meta.shards {
            let last_delta = shard_hist.deltas.last().unwrap();
            let replicas = &last_delta.replicas_on_backup_taken;

            // Parse listen addrs into TcpAddress for membership.
            let addrs: Vec<TcpAddress> = replicas
                .iter()
                .map(|s| {
                    let sa: std::net::SocketAddr = s.parse().map_err(|e| {
                        CloneError::AdminError(format!("invalid replica addr '{s}': {e}"))
                    })?;
                    Ok(TcpAddress(sa))
                })
                .collect::<Result<Vec<_>, CloneError>>()?;

            let shard = crate::ShardNumber(shard_hist.shard);
            let membership = IrMembership::new(addrs);
            transport.set_shard_addresses(shard, membership.clone());

            let client = crate::tapir::ShardClient::new(
                self.rng.fork(),
                IrClientId::new(&mut self.rng),
                shard,
                membership,
                transport.clone(),
            );

            for delta in &shard_hist.deltas {
                let file_path = format!("{backup_dir}/{}", delta.file);
                let delta_bytes = std::fs::read(&file_path).map_err(|e| {
                    CloneError::AdminError(format!("read {}: {e}", delta.file))
                })?;

                let deltas: Vec<LeaderRecordDelta<String, String>> =
                    bitcode::deserialize(&delta_bytes).map_err(|e| {
                        CloneError::AdminError(format!("deserialize {}: {e}", delta.file))
                    })?;

                let changes: Vec<_> =
                    deltas.into_iter().flat_map(|d| d.changes).collect();
                ship_changes(&client, shard, &changes, &mut self.rng).await;
            }
        }

        // No Phase 3 — solo clusters don't use discovery registration.
        self.report_progress("restore-direct:complete");
        Ok(())
    }
}
