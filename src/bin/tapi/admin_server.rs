use crate::node::Node;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tapirs::ShardNumber;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

#[derive(Deserialize)]
struct AdminRequest {
    command: String,
    #[serde(default)]
    shard: Option<u32>,
    #[serde(default)]
    listen_addr: Option<String>,
    #[serde(default)]
    storage: Option<String>,
    #[serde(default)]
    backup: Option<crate::node::ShardBackup>,
    #[serde(default)]
    new_membership: Option<Vec<String>>,
    /// Static membership for add_replica. When provided, creates the replica
    /// with the specified membership directly (no shard-manager involvement).
    /// When absent, uses create_replica() which coordinates via shard-manager.
    #[serde(default)]
    membership: Option<Vec<String>>,
}

#[derive(Serialize)]
struct AdminResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    shards: Option<Vec<ShardInfo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backup: Option<crate::node::ShardBackup>,
}

#[derive(Serialize)]
struct ShardInfo {
    shard: u32,
    listen_addr: String,
}

pub async fn start(addr: std::net::SocketAddr, node: Arc<Node>) {
    let listener = TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("admin: failed to bind {addr}: {e}"));
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let node = Arc::clone(&node);
                    tokio::spawn(async move {
                        let (reader, mut writer) = stream.into_split();
                        let mut lines = BufReader::new(reader).lines();
                        while let Ok(Some(line)) = lines.next_line().await {
                            let resp = handle_request(&node, &line).await;
                            let mut out = serde_json::to_string(&resp).unwrap();
                            out.push('\n');
                            let _ = writer.write_all(out.as_bytes()).await;
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!("admin accept error: {e}");
                }
            }
        }
    });
}

async fn handle_request(node: &Node, line: &str) -> AdminResponse {
    let req: AdminRequest = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            return AdminResponse {
                ok: false,
                message: Some(format!("invalid JSON: {e}")),
                shards: None,
                backup: None,
            }
        }
    };

    match req.command.as_str() {
        "status" => {
            let list = node.shard_list();
            AdminResponse {
                ok: true,
                message: Some(format!("{} replica(s) running", list.len())),
                shards: Some(
                    list.into_iter()
                        .map(|(shard, addr)| ShardInfo {
                            shard: shard.0,
                            listen_addr: addr.to_string(),
                        })
                        .collect(),
                ),
                backup: None,
            }
        }
        "add_replica" => {
            let Some(shard_id) = req.shard else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'shard' field".into()),
                    shards: None,
                    backup: None,
                };
            };
            let Some(ref listen_addr_str) = req.listen_addr else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'listen_addr' field".into()),
                    shards: None,
                    backup: None,
                };
            };
            let listen_addr: std::net::SocketAddr = match listen_addr_str.parse() {
                Ok(a) => a,
                Err(e) => {
                    return AdminResponse {
                        ok: false,
                        message: Some(format!("invalid listen_addr: {e}")),
                        shards: None,
                        backup: None,
                    };
                }
            };
            if let Some(membership_strs) = req.membership {
                // Static add with explicit membership (no shard-manager).
                let cfg = crate::config::ReplicaConfig {
                    shard: shard_id,
                    listen_addr: listen_addr_str.clone(),
                    membership: membership_strs,
                };
                match node.add_replica(&cfg).await {
                    Ok(()) => AdminResponse {
                        ok: true,
                        message: Some(format!("replica for shard {shard_id} added with static membership")),
                        shards: None,
                        backup: None,
                    },
                    Err(e) => AdminResponse {
                        ok: false,
                        message: Some(format!("add_replica failed: {e}")),
                        shards: None,
                        backup: None,
                    },
                }
            } else {
                // Dynamic add via shard-manager.
                let storage_str = req.storage.as_deref().unwrap_or("memory");
                match node.create_replica(ShardNumber(shard_id), listen_addr, storage_str).await {
                    Ok(()) => AdminResponse {
                        ok: true,
                        message: Some(format!("replica for shard {shard_id} created")),
                        shards: None,
                        backup: None,
                    },
                    Err(e) => AdminResponse {
                        ok: false,
                        message: Some(format!("create_replica failed: {e}")),
                        shards: None,
                        backup: None,
                    },
                }
            }
        }
        "view_change" => {
            let Some(shard_id) = req.shard else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'shard' field".into()),
                    shards: None,
                    backup: None,
                };
            };
            let ok = node.force_view_change(ShardNumber(shard_id));
            AdminResponse {
                ok,
                message: Some(if ok {
                    "view change triggered".into()
                } else {
                    format!("shard {shard_id} not found")
                }),
                shards: None,
                backup: None,
            }
        }
        "remove_replica" => {
            let Some(shard_id) = req.shard else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'shard' field".into()),
                    shards: None,
                    backup: None,
                };
            };
            let ok = node.remove_replica(ShardNumber(shard_id));
            AdminResponse {
                ok,
                message: Some(if ok {
                    "replica removed".into()
                } else {
                    format!("shard {shard_id} not found")
                }),
                shards: None,
                backup: None,
            }
        }
        "leave" => {
            let Some(shard_id) = req.shard else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'shard' field".into()),
                    shards: None,
                    backup: None,
                };
            };
            match node.leave_shard(ShardNumber(shard_id)).await {
                Ok(()) => AdminResponse {
                    ok: true,
                    message: Some(format!("left shard {shard_id}")),
                    shards: None,
                    backup: None,
                },
                Err(e) => AdminResponse {
                    ok: false,
                    message: Some(format!("leave failed: {e}")),
                    shards: None,
                    backup: None,
                },
            }
        }
        "backup_shard" => {
            let Some(shard_id) = req.shard else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'shard' field".into()),
                    shards: None,
                    backup: None,
                };
            };
            match node.backup_shard(ShardNumber(shard_id)).await {
                Some(backup) => AdminResponse {
                    ok: true,
                    message: Some(format!("shard {shard_id} backed up")),
                    shards: None,
                    backup: Some(backup),
                },
                None => AdminResponse {
                    ok: false,
                    message: Some(format!("shard {shard_id} not found or backup failed")),
                    shards: None,
                    backup: None,
                },
            }
        }
        "restore_shard" => {
            let Some(shard_id) = req.shard else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'shard' field".into()),
                    shards: None,
                    backup: None,
                };
            };
            let Some(listen_addr_str) = req.listen_addr else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'listen_addr' field".into()),
                    shards: None,
                    backup: None,
                };
            };
            let listen_addr: std::net::SocketAddr = match listen_addr_str.parse() {
                Ok(a) => a,
                Err(e) => {
                    return AdminResponse {
                        ok: false,
                        message: Some(format!("invalid listen_addr: {e}")),
                        shards: None,
                        backup: None,
                    };
                }
            };
            let Some(backup) = req.backup else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'backup' field".into()),
                    shards: None,
                    backup: None,
                };
            };
            let Some(new_membership_strs) = req.new_membership else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'new_membership' field".into()),
                    shards: None,
                    backup: None,
                };
            };
            let new_membership: Vec<std::net::SocketAddr> = match new_membership_strs
                .iter()
                .map(|a| {
                    a.parse()
                        .map_err(|e| format!("invalid membership addr '{a}': {e}"))
                })
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(addrs) => addrs,
                Err(e) => {
                    return AdminResponse {
                        ok: false,
                        message: Some(e),
                        shards: None,
                        backup: None,
                    };
                }
            };
            match node
                .restore_shard(ShardNumber(shard_id), listen_addr, &backup, new_membership)
                .await
            {
                Ok(()) => AdminResponse {
                    ok: true,
                    message: Some(format!("shard {shard_id} restored at {listen_addr}")),
                    shards: None,
                    backup: None,
                },
                Err(e) => AdminResponse {
                    ok: false,
                    message: Some(format!("restore_shard failed: {e}")),
                    shards: None,
                    backup: None,
                },
            }
        }
        other => AdminResponse {
            ok: false,
            message: Some(format!("unknown command: {other}")),
            shards: None,
            backup: None,
        },
    }
}
