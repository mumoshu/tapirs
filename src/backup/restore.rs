use super::storage::BackupStorage;
use super::types::ClusterMetadata;

impl super::BackupManager {
    /// Restore a cluster from backup files.
    ///
    /// Three-phase process:
    /// 1. **Prepare**: Create empty replicas on target nodes via admin API.
    /// 2. **Apply**: Ship delta data to each shard via shard manager `/v1/apply-changes`.
    /// 3. **Finalize**: Register shards as Active via shard manager `/v1/register`.
    ///
    /// `admin_addrs` are the admin API addresses of the target nodes (one per node).
    /// The number of admin addresses must match the number of replicas per shard
    /// recorded in the backup.
    pub async fn restore_cluster<S: BackupStorage>(
        &self,
        admin_addrs: &[String],
        storage: &S,
    ) -> Result<(), String> {
        // Read cluster.json.
        let data = storage.read_string("cluster.json").await?;
        let meta: ClusterMetadata =
            serde_json::from_str(&data).map_err(|e| format!("parse cluster.json: {e}"))?;

        if meta.shards.is_empty() {
            return Err("no shards in backup".to_string());
        }

        // Phase 1: Prepare — create empty replicas on target nodes.
        for shard_hist in &meta.shards {
            let last_delta = shard_hist
                .deltas
                .last()
                .ok_or_else(|| format!("shard {} has no deltas", shard_hist.shard))?;

            let replicas = &last_delta.replicas_on_backup_taken;
            if admin_addrs.len() < replicas.len() {
                return Err(format!(
                    "shard {} has {} replicas but only {} admin addrs provided",
                    shard_hist.shard,
                    replicas.len(),
                    admin_addrs.len()
                ));
            }

            // Pair each admin addr with a listen addr from the backup.
            let membership_json: Vec<String> = replicas.clone();
            for (i, listen_addr) in replicas.iter().enumerate() {
                let admin_addr = &admin_addrs[i];
                let request = serde_json::json!({
                    "command": "add_replica",
                    "shard": shard_hist.shard,
                    "listen_addr": listen_addr,
                    "membership": membership_json,
                });
                let request_json = request.to_string();

                #[cfg(feature = "tls")]
                let resp = {
                    crate::node::node_client::admin_request(
                        admin_addr,
                        &request_json,
                        &self.admin_tls_connector,
                    )
                    .await
                    .map_err(|e| {
                        format!(
                            "create replica on {admin_addr} for shard {}: {e}",
                            shard_hist.shard
                        )
                    })?
                };

                #[cfg(not(feature = "tls"))]
                let resp = {
                    crate::node::node_client::send_admin_request(admin_addr, &request_json)
                        .await
                        .map_err(|e| {
                            format!(
                                "create replica on {admin_addr} for shard {}: {e}",
                                shard_hist.shard
                            )
                        })?
                };

                if !resp.ok {
                    return Err(format!(
                        "create replica on {admin_addr} for shard {}: {}",
                        shard_hist.shard,
                        resp.message.as_deref().unwrap_or("unknown error")
                    ));
                }
            }
        }

        // Phase 2: Apply — ship delta data to each shard.
        for shard_hist in &meta.shards {
            let last_delta = shard_hist.deltas.last().unwrap();
            let replicas = &last_delta.replicas_on_backup_taken;

            for delta in &shard_hist.deltas {
                let delta_bytes = storage.read(&delta.file).await?;

                self.shard_manager_client
                    .apply_changes(shard_hist.shard, replicas, &delta_bytes)
                    .map_err(|e| {
                        format!(
                            "apply delta {} to shard {}: {e}",
                            delta.file, shard_hist.shard
                        )
                    })?;
            }
        }

        // Phase 3: Finalize — register shards in discovery.
        for shard_hist in &meta.shards {
            let last_delta = shard_hist.deltas.last().unwrap();
            let replicas = &last_delta.replicas_on_backup_taken;

            self.shard_manager_client
                .register(
                    shard_hist.shard,
                    last_delta.key_range_start_on_backup_taken.as_deref(),
                    last_delta.key_range_end_on_backup_taken.as_deref(),
                    Some(replicas),
                )
                .map_err(|e| format!("register shard {}: {e}", shard_hist.shard))?;
        }

        Ok(())
    }
}
