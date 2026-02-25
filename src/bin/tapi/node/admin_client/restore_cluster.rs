use std::collections::BTreeMap;

/// Restore a full cluster from a backup directory onto target nodes.
///
/// Precondition: the operator has started empty nodes (no shards) and
/// provides their admin addresses.
///
/// Orchestration flow:
///   1. Read `<backup_dir>/cluster.json` for shard list and replica counts
///   2. Read each `<backup_dir>/shard_<id>.json` into memory
///   3. For each shard s (in shard-ID order):
///      a. Assign replicas round-robin: replica r -> node (r % num_nodes)
///      (a single node may host multiple replicas of the same shard --
///      TAPIR replicas are single-threaded, so this effectively
///      utilizes multiple CPU cores on one machine)
///      b. Each replica gets a unique listen addr: node_ip:port where
///      port is allocated sequentially from base_port across all
///      (shard, replica) pairs on that node
///      c. Compute new_membership = all listen addrs for this shard
///      d. Send restore_shard command to each assigned node's admin server
///      e. Optionally register the shard with the discovery service
///
/// After restore, the cluster is operational: replicas replay state
/// from the backup via BootstrapRecord -> StartView -> sync(), and
/// clients can discover the new topology via the discovery service.
pub(super) async fn restore_cluster(
    admin_addrs: Vec<String>,
    backup_dir: &str,
    base_port: u16,
    #[cfg(feature = "tls")] tls_connector: &Option<tapirs::tls::ReloadableTlsConnector>,
) -> Result<(), String> {
    // 1. Read cluster metadata.
    let meta_path = format!("{backup_dir}/cluster.json");
    let meta_json =
        tokio::fs::read_to_string(&meta_path).await.map_err(|e| format!("read {meta_path}: {e}"))?;
    let metadata: super::ClusterMetadata =
        serde_json::from_str(&meta_json).map_err(|e| format!("parse {meta_path}: {e}"))?;

    let num_nodes = admin_addrs.len();
    println!(
        "Restoring {} shard(s) to {} node(s)",
        metadata.shards.len(),
        num_nodes
    );

    // 2. Read all shard backups.
    let mut shard_backups: BTreeMap<u32, crate::node::ShardBackup> = BTreeMap::new();
    for &shard_id in &metadata.shards {
        let path = format!("{backup_dir}/shard_{shard_id}.json");
        let json = tokio::fs::read_to_string(&path).await.map_err(|e| format!("read {path}: {e}"))?;
        let backup: crate::node::ShardBackup =
            serde_json::from_str(&json).map_err(|e| format!("parse {path}: {e}"))?;
        shard_backups.insert(shard_id, backup);
    }

    // Parse admin addresses to extract IPs for listen addr computation.
    let admin_sockaddrs: Vec<std::net::SocketAddr> = admin_addrs
        .iter()
        .map(|a| {
            a.parse()
                .map_err(|e| format!("invalid admin addr '{a}': {e}"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Track per-node port counter for sequential allocation.
    let mut node_next_port: Vec<u16> = vec![base_port; num_nodes];

    // 3. For each shard, determine replica placement and restore.
    for (&shard_id, backup) in &shard_backups {
        let original_replicas = metadata
            .replicas_per_shard
            .get(&shard_id)
            .copied()
            .unwrap_or(3);

        // Assign replicas round-robin across nodes.
        // A single node may host multiple replicas (TAPIR replicas are
        // single-threaded, so this utilizes multiple CPU cores).
        let mut new_membership: Vec<String> = Vec::new();
        let mut assignments: Vec<(usize, String)> = Vec::new();

        for replica_idx in 0..original_replicas {
            let node_idx = replica_idx % num_nodes;
            let ip = admin_sockaddrs[node_idx].ip();
            let port = node_next_port[node_idx];
            node_next_port[node_idx] += 1;
            let listen_addr = format!("{ip}:{port}");
            new_membership.push(listen_addr.clone());
            assignments.push((node_idx, listen_addr));
        }

        println!(
            "Shard {shard_id}: restoring {} replica(s)",
            original_replicas
        );

        // Send restore_shard to each assigned node.
        for (node_idx, listen_addr) in &assignments {
            let req = super::RestoreRequest {
                command: "restore_shard",
                shard: shard_id,
                listen_addr,
                backup,
                new_membership: &new_membership,
            };
            let req_str = serde_json::to_string(&req)
                .map_err(|e| format!("serialize restore request: {e}"))?;

            let resp = super::admin_request(
                &admin_addrs[*node_idx],
                &req_str,
                #[cfg(feature = "tls")]
                tls_connector,
            )
            .await?;
            if !resp.ok {
                return Err(format!(
                    "restore_shard for shard {shard_id} on {} failed: {:?}",
                    admin_addrs[*node_idx], resp.message
                ));
            }
            println!(
                "  Restored shard {shard_id} on {} at {listen_addr}",
                admin_addrs[*node_idx]
            );
        }

    }

    println!("Cluster restore complete.");
    Ok(())
}
