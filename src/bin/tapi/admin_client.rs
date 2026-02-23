use crate::AdminAction;
use crate::node::ShardBackup;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use tapirs::node::admin_client::send_admin_request;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// Cluster metadata stored in cluster.json.
#[derive(Serialize, Deserialize)]
struct ClusterMetadata {
    shards: Vec<u32>,
    replicas_per_shard: HashMap<u32, usize>,
}

/// Send a parsed admin request, using TLS if configured.
async fn admin_request(
    addr: &str,
    request_json: &str,
    #[cfg(feature = "tls")] tls_connector: &Option<tapirs::tls::ReloadableTlsConnector>,
) -> Result<tapirs::node::admin_client::AdminStatusResponse, String> {
    #[cfg(feature = "tls")]
    if let Some(connector) = tls_connector {
        return tapirs::node::admin_client::send_admin_request_tls(
            addr,
            request_json,
            connector,
        )
        .await;
    }
    send_admin_request(addr, request_json).await
}

/// Exchange a raw admin request, returning the unparsed response line.
///
/// Used for WaitReady (graceful connection failure) and the generic
/// single-node path (raw JSON pretty-printing).
async fn raw_admin_exchange(
    addr: &str,
    request: &str,
    #[cfg(feature = "tls")] tls_connector: &Option<tapirs::tls::ReloadableTlsConnector>,
) -> Result<Option<String>, String> {
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connect to {addr}: {e}"))?;

    let mut line = request.to_string();
    line.push('\n');

    #[cfg(feature = "tls")]
    if let Some(connector) = tls_connector {
        let tls_conn = connector.connector();
        let host = addr.split(':').next().unwrap_or(addr).to_string();
        let server_name = rustls::pki_types::ServerName::try_from(host)
            .map_err(|e| format!("invalid server name: {e}"))?;
        let tls_stream = tls_conn
            .connect(server_name, stream)
            .await
            .map_err(|e| format!("TLS connect to {addr}: {e}"))?;
        let (reader, mut writer) = tokio::io::split(tls_stream);
        writer
            .write_all(line.as_bytes())
            .await
            .map_err(|e| format!("send to {addr}: {e}"))?;
        let mut lines = BufReader::new(reader).lines();
        return lines
            .next_line()
            .await
            .map_err(|e| format!("read from {addr}: {e}"));
    }

    let (reader, mut writer) = stream.into_split();
    writer
        .write_all(line.as_bytes())
        .await
        .map_err(|e| format!("send to {addr}: {e}"))?;
    let mut lines = BufReader::new(reader).lines();
    lines
        .next_line()
        .await
        .map_err(|e| format!("read from {addr}: {e}"))
}

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
async fn backup_cluster(
    admin_addrs: Vec<String>,
    output_dir: &str,
    #[cfg(feature = "tls")] tls_connector: &Option<tapirs::tls::ReloadableTlsConnector>,
) -> Result<(), String> {
    // 1. Query each node for status to discover which shards exist.
    let mut shard_to_nodes: BTreeMap<u32, Vec<String>> = BTreeMap::new();
    for addr in &admin_addrs {
        let resp = admin_request(
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
        let resp = admin_request(
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
        let resp = admin_request(
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
    let metadata = ClusterMetadata {
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

#[derive(Serialize)]
struct RestoreRequest<'a> {
    command: &'static str,
    shard: u32,
    listen_addr: &'a str,
    backup: &'a ShardBackup,
    new_membership: &'a [String],
}

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
async fn restore_cluster(
    admin_addrs: Vec<String>,
    backup_dir: &str,
    base_port: u16,
    #[cfg(feature = "tls")] tls_connector: &Option<tapirs::tls::ReloadableTlsConnector>,
) -> Result<(), String> {
    // 1. Read cluster metadata.
    let meta_path = format!("{backup_dir}/cluster.json");
    let meta_json =
        tokio::fs::read_to_string(&meta_path).await.map_err(|e| format!("read {meta_path}: {e}"))?;
    let metadata: ClusterMetadata =
        serde_json::from_str(&meta_json).map_err(|e| format!("parse {meta_path}: {e}"))?;

    let num_nodes = admin_addrs.len();
    println!(
        "Restoring {} shard(s) to {} node(s)",
        metadata.shards.len(),
        num_nodes
    );

    // 2. Read all shard backups.
    let mut shard_backups: BTreeMap<u32, ShardBackup> = BTreeMap::new();
    for &shard_id in &metadata.shards {
        let path = format!("{backup_dir}/shard_{shard_id}.json");
        let json = tokio::fs::read_to_string(&path).await.map_err(|e| format!("read {path}: {e}"))?;
        let backup: ShardBackup =
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
            let req = RestoreRequest {
                command: "restore_shard",
                shard: shard_id,
                listen_addr,
                backup,
                new_membership: &new_membership,
            };
            let req_str = serde_json::to_string(&req)
                .map_err(|e| format!("serialize restore request: {e}"))?;

            let resp = admin_request(
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

pub async fn run(
    action: AdminAction,
    #[cfg(feature = "tls")] tls_config: Option<tapirs::tls::TlsConfig>,
) {
    #[cfg(feature = "tls")]
    let tls_connector = tls_config.as_ref().map(|c| {
        tapirs::tls::ReloadableTlsConnector::new(c)
            .unwrap_or_else(|e| panic!("admin TLS config error: {e}"))
    });

    // Multi-node operations with their own orchestration.
    match &action {
        AdminAction::Backup {
            admin_addrs,
            output,
        } => {
            let addrs: Vec<String> =
                admin_addrs.split(',').map(|s| s.trim().to_string()).collect();
            if let Err(e) = backup_cluster(
                addrs,
                output,
                #[cfg(feature = "tls")]
                &tls_connector,
            )
            .await
            {
                eprintln!("Backup failed: {e}");
                std::process::exit(1);
            }
            return;
        }
        AdminAction::Restore {
            backup_dir,
            admin_addrs,
            base_port,
        } => {
            let addrs: Vec<String> =
                admin_addrs.split(',').map(|s| s.trim().to_string()).collect();
            if let Err(e) = restore_cluster(
                addrs,
                backup_dir,
                *base_port,
                #[cfg(feature = "tls")]
                &tls_connector,
            )
            .await
            {
                eprintln!("Restore failed: {e}");
                std::process::exit(1);
            }
            return;
        }
        _ => {}
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
            storage,
            membership,
        } => {
            let storage_str = match storage {
                crate::StorageBackend::Memory => "memory",
                crate::StorageBackend::Disk => "disk",
            };
            let request = if membership.is_empty() {
                format!(
                    r#"{{"command":"add_replica","shard":{shard},"listen_addr":"{listen_addr}","storage":"{storage_str}"}}"#
                )
            } else {
                let membership_json: Vec<String> =
                    membership.iter().map(|a| format!("\"{}\"", a)).collect();
                format!(
                    r#"{{"command":"add_replica","shard":{shard},"listen_addr":"{listen_addr}","storage":"{storage_str}","membership":[{}]}}"#,
                    membership_json.join(",")
                )
            };
            (admin_listen_addr, request)
        }
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
        AdminAction::Leave {
            admin_listen_addr,
            shard,
        } => (
            admin_listen_addr,
            format!(r#"{{"command":"leave","shard":{shard}}}"#),
        ),
        AdminAction::WaitReady {
            admin_listen_addr,
            timeout,
        } => {
            let deadline =
                std::time::Instant::now() + std::time::Duration::from_secs(timeout);
            loop {
                if let Ok(Some(resp)) = raw_admin_exchange(
                    &admin_listen_addr,
                    r#"{"command":"status"}"#,
                    #[cfg(feature = "tls")]
                    &tls_connector,
                )
                .await
                    && serde_json::from_str::<serde_json::Value>(&resp).is_ok()
                {
                    println!("ready");
                    return;
                }
                if std::time::Instant::now() >= deadline {
                    eprintln!(
                        "timeout: admin server at {} not ready after {}s",
                        admin_listen_addr, timeout
                    );
                    std::process::exit(1);
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
        AdminAction::Backup { .. } | AdminAction::Restore { .. } => unreachable!(),
    };

    match raw_admin_exchange(
        &addr,
        &request,
        #[cfg(feature = "tls")]
        &tls_connector,
    )
    .await
    {
        Ok(Some(response)) => {
            // Pretty-print if it's valid JSON, otherwise print raw.
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&response) {
                println!("{}", serde_json::to_string_pretty(&json).unwrap());
            } else {
                println!("{response}");
            }
        }
        Ok(None) => {
            eprintln!("no response from {addr}");
        }
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
}
