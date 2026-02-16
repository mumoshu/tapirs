use crate::AdminAction;
use crate::node::ShardBackup;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[derive(Deserialize)]
struct AdminStatusResponse {
    ok: bool,
    message: Option<String>,
    shards: Option<Vec<ShardInfoResponse>>,
    backup: Option<ShardBackup>,
}

#[derive(Deserialize)]
struct ShardInfoResponse {
    shard: u32,
    #[allow(dead_code)]
    listen_addr: String,
}

/// Cluster metadata stored in cluster.json.
#[derive(Serialize, Deserialize)]
struct ClusterMetadata {
    shards: Vec<u32>,
    replicas_per_shard: HashMap<u32, usize>,
}

async fn send_admin_request(addr: &str, request_json: &str) -> Result<AdminStatusResponse, String> {
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connect to {addr}: {e}"))?;
    let (reader, mut writer) = stream.into_split();

    let mut line = request_json.to_string();
    line.push('\n');
    writer
        .write_all(line.as_bytes())
        .await
        .map_err(|e| format!("send to {addr}: {e}"))?;

    let mut lines = BufReader::new(reader).lines();
    let response_line = lines
        .next_line()
        .await
        .map_err(|e| format!("read from {addr}: {e}"))?
        .ok_or_else(|| format!("no response from {addr}"))?;

    serde_json::from_str(&response_line).map_err(|e| format!("parse response from {addr}: {e}"))
}

/// Back up all shards in a running cluster to a directory.
///
/// Orchestration flow:
///   1. Query each node's admin server for status (hosted shards)
///   2. Deduplicate: build a map of shard -> list of hosting nodes
///   3. For each unique shard (in shard-ID order):
///      a. Force view change on one node -- synchronizes the IR record
///         so the backup captures all committed operations
///      b. Wait for view change to settle (5 seconds)
///      c. Send backup_shard command to the same node
///      d. Write the ShardBackup JSON to `<output>/shard_<id>.json`
///   4. Write `<output>/cluster.json` with shard list and replica counts
///
/// The backup directory can later be used with `restore_cluster` to
/// rebuild the cluster on fresh nodes.
async fn backup_cluster(admin_addrs: Vec<String>, output_dir: &str) -> Result<(), String> {
    // 1. Query each node for status to discover which shards exist.
    let mut shard_to_nodes: BTreeMap<u32, Vec<String>> = BTreeMap::new();
    for addr in &admin_addrs {
        let resp = send_admin_request(addr, r#"{"command":"status"}"#).await?;
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
    std::fs::create_dir_all(output_dir)
        .map_err(|e| format!("create output dir '{}': {e}", output_dir))?;

    // 3. For each shard: force view change, wait, then backup.
    let mut replicas_per_shard = HashMap::new();
    for (shard_id, nodes) in &shard_to_nodes {
        replicas_per_shard.insert(*shard_id, nodes.len());

        println!("Shard {shard_id}: forcing view change...");
        let vc_req = format!(r#"{{"command":"view_change","shard":{shard_id}}}"#);
        let resp = send_admin_request(&nodes[0], &vc_req).await?;
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
        let resp = send_admin_request(&nodes[0], &backup_req).await?;
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
        std::fs::write(&path, json).map_err(|e| format!("write {path}: {e}"))?;
        println!("Shard {shard_id}: backup written to {path}");
    }

    // 4. Write cluster metadata.
    let metadata = ClusterMetadata {
        shards: shard_to_nodes.keys().copied().collect(),
        replicas_per_shard,
    };
    let meta_path = format!("{output_dir}/cluster.json");
    let meta_json = serde_json::to_string_pretty(&metadata)
        .map_err(|e| format!("serialize cluster metadata: {e}"))?;
    std::fs::write(&meta_path, meta_json).map_err(|e| format!("write {meta_path}: {e}"))?;

    println!(
        "Cluster backup complete: {} shard(s) written to {output_dir}/",
        shard_to_nodes.len()
    );
    Ok(())
}

pub async fn run(action: AdminAction) {
    // Multi-node operations with their own orchestration.
    if let AdminAction::Backup {
        admin_addrs,
        output,
    } = &action
    {
        let addrs: Vec<String> = admin_addrs.split(',').map(|s| s.trim().to_string()).collect();
        if let Err(e) = backup_cluster(addrs, output).await {
            eprintln!("Backup failed: {e}");
            std::process::exit(1);
        }
        return;
    }

    // Single-node operations (existing code).
    let (addr, request) = match action {
        AdminAction::Status { admin_listen_addr } => (
            admin_listen_addr,
            r#"{"command":"status"}"#.to_string(),
        ),
        AdminAction::AddReplica {
            admin_listen_addr,
            shard,
            listen_addr,
        } => (
            admin_listen_addr,
            format!(
                r#"{{"command":"add_replica","shard":{shard},"listen_addr":"{listen_addr}"}}"#
            ),
        ),
        AdminAction::RemoveReplica {
            admin_listen_addr,
            shard,
        } => (
            admin_listen_addr,
            format!(r#"{{"command":"remove_replica","shard":{shard}}}"#),
        ),
        AdminAction::ViewChange {
            admin_listen_addr,
            shard,
        } => (
            admin_listen_addr,
            format!(r#"{{"command":"view_change","shard":{shard}}}"#),
        ),
        AdminAction::Backup { .. } => unreachable!(),
    };

    let stream = TcpStream::connect(&addr)
        .await
        .unwrap_or_else(|e| panic!("failed to connect to admin at {addr}: {e}"));
    let (reader, mut writer) = stream.into_split();

    let mut line = request;
    line.push('\n');
    writer
        .write_all(line.as_bytes())
        .await
        .expect("failed to send request");

    let mut lines = BufReader::new(reader).lines();
    if let Ok(Some(response)) = lines.next_line().await {
        // Pretty-print if it's valid JSON, otherwise print raw.
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&response) {
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
        } else {
            println!("{response}");
        }
    }
}
