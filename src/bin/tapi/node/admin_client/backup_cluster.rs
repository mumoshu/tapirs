use std::collections::{BTreeMap, HashMap};

/// Back up all shards in a running cluster to a directory.
///
/// Orchestration flow:
///   1. Query each node's admin server for status (hosted shards)
///   2. Deduplicate: build a map of shard -> list of hosting nodes
///   3. For each unique shard (in shard-ID order):
///      a. Force view change on one node -- synchronizes the IR record
///      so the backup captures all committed operations
///      b. Wait for view change to settle (5 seconds)
///      c. Send backup_shard command to the same node
///      d. Write the ShardBackup JSON to `<output>/shard_<id>.json`
///   4. Write `<output>/cluster.json` with shard list and replica counts
///
/// The backup directory can later be used with `restore_cluster` to
/// rebuild the cluster on fresh nodes.
pub(super) async fn backup_cluster(
    admin_addrs: Vec<String>,
    output_dir: &str,
    #[cfg(feature = "tls")] tls_connector: &Option<tapirs::tls::ReloadableTlsConnector>,
) -> Result<(), String> {
    // 1. Query each node for status to discover which shards exist.
    let mut shard_to_nodes: BTreeMap<u32, Vec<String>> = BTreeMap::new();
    for addr in &admin_addrs {
        let resp = super::admin_request(
            addr,
            r#"{"command":"status"}"#,
            #[cfg(feature = "tls")]
            tls_connector,
        )
        .await?;
        if !resp.ok {
            return Err(format!("status failed on {addr}: {:?}", resp.message));
        }
        if let Some(shards) = resp.shards {
            for s in shards {
                shard_to_nodes
                    .entry(s.shard)
                    .or_default()
                    .push(addr.clone());
            }
        }
    }

    if shard_to_nodes.is_empty() {
        return Err("no shards found across any node".to_string());
    }

    println!(
        "Found {} unique shard(s): {:?}",
        shard_to_nodes.len(),
        shard_to_nodes.keys().collect::<Vec<_>>()
    );

    // 2. Create output directory.
    tokio::fs::create_dir_all(output_dir)
        .await
        .map_err(|e| format!("create output dir '{}': {e}", output_dir))?;

    // 3. For each shard: force view change, wait, then backup.
    let mut replicas_per_shard = HashMap::new();
    for (shard_id, nodes) in &shard_to_nodes {
        replicas_per_shard.insert(*shard_id, nodes.len());

        println!("Shard {shard_id}: forcing view change...");
        let vc_req = format!(r#"{{"command":"view_change","shard":{shard_id}}}"#);
        let resp = super::admin_request(
            &nodes[0],
            &vc_req,
            #[cfg(feature = "tls")]
            tls_connector,
        )
        .await?;
        if !resp.ok {
            return Err(format!(
                "view_change for shard {shard_id} failed: {:?}",
                resp.message
            ));
        }

        println!("Shard {shard_id}: waiting for view change to settle...");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        println!("Shard {shard_id}: backing up...");
        let backup_req = format!(r#"{{"command":"backup_shard","shard":{shard_id}}}"#);
        let resp = super::admin_request(
            &nodes[0],
            &backup_req,
            #[cfg(feature = "tls")]
            tls_connector,
        )
        .await?;
        if !resp.ok {
            return Err(format!(
                "backup_shard for shard {shard_id} failed: {:?}",
                resp.message
            ));
        }
        let backup = resp
            .backup
            .ok_or_else(|| format!("no backup data returned for shard {shard_id}"))?;

        let path = format!("{output_dir}/shard_{shard_id}.json");
        let json = serde_json::to_string_pretty(&backup)
            .map_err(|e| format!("serialize shard {shard_id} backup: {e}"))?;
        tokio::fs::write(&path, json).await.map_err(|e| format!("write {path}: {e}"))?;
        println!("Shard {shard_id}: backup written to {path}");
    }

    // 4. Write cluster metadata.
    let metadata = super::ClusterMetadata {
        shards: shard_to_nodes.keys().copied().collect(),
        replicas_per_shard,
    };
    let meta_path = format!("{output_dir}/cluster.json");
    let meta_json = serde_json::to_string_pretty(&metadata)
        .map_err(|e| format!("serialize cluster metadata: {e}"))?;
    tokio::fs::write(&meta_path, meta_json).await.map_err(|e| format!("write {meta_path}: {e}"))?;

    println!(
        "Cluster backup complete: {} shard(s) written to {output_dir}/",
        shard_to_nodes.len()
    );
    Ok(())
}
