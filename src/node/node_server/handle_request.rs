use super::types::{AdminRequest, AdminResponse, ShardInfo};
use crate::node::types::ReplicaConfig;
use crate::node::Node;
use crate::ShardNumber;

pub async fn handle_request(node: &Node, line: &str) -> AdminResponse {
    let req: AdminRequest = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            return AdminResponse {
                ok: false,
                message: Some(format!("invalid JSON: {e}")),
                shards: None,
            }
        }
    };

    match req.command.as_str() {
        "status" => {
            let list = node.shard_list();
            let read_list = node.read_replica_list();
            let total = list.len() + read_list.len();
            let mut shards: Vec<ShardInfo> = list
                .into_iter()
                .map(|(shard, addr)| ShardInfo {
                    shard: shard.0,
                    listen_addr: addr.to_string(),
                })
                .collect();
            for (shard, addr) in read_list {
                shards.push(ShardInfo {
                    shard: shard.0,
                    listen_addr: format!("{addr} (read-only)"),
                });
            }
            AdminResponse {
                ok: true,
                message: Some(format!("{total} replica(s) running")),
                shards: Some(shards),
            }
        }
        "add_replica" => {
            let Some(shard_id) = req.shard else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'shard' field".into()),
                    shards: None,
                };
            };
            let Some(ref listen_addr_str) = req.listen_addr else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'listen_addr' field".into()),
                    shards: None,
                };
            };
            let listen_addr: std::net::SocketAddr = match listen_addr_str.parse() {
                Ok(a) => a,
                Err(e) => {
                    return AdminResponse {
                        ok: false,
                        message: Some(format!("invalid listen_addr: {e}")),
                        shards: None,
                    };
                }
            };
            if let Some(membership_strs) = req.membership {
                // Static add with explicit membership (no shard-manager).
                let cfg = ReplicaConfig {
                    shard: shard_id,
                    listen_addr: listen_addr_str.clone(),
                    membership: membership_strs,
                };
                match node.add_replica_no_join(&cfg).await {
                    Ok(()) => AdminResponse {
                        ok: true,
                        message: Some(format!("replica for shard {shard_id} added with static membership")),
                        shards: None,
                    },
                    Err(e) => AdminResponse {
                        ok: false,
                        message: Some(format!("add_replica failed: {e}")),
                        shards: None,
                    },
                }
            } else {
                // Dynamic add via shard-manager.
                match node.add_replica_join(ShardNumber(shard_id), listen_addr).await {
                    Ok(()) => AdminResponse {
                        ok: true,
                        message: Some(format!("replica for shard {shard_id} created")),
                        shards: None,
                    },
                    Err(e) => AdminResponse {
                        ok: false,
                        message: Some(format!("add_replica_join failed: {e}")),
                        shards: None,
                    },
                }
            }
        }
        "add_writable_clone_from_s3" => {
            let Some(shard_id) = req.shard else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'shard' field".into()),
                    shards: None,
                };
            };
            let Some(ref listen_addr_str) = req.listen_addr else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'listen_addr' field".into()),
                    shards: None,
                };
            };
            let Some(membership_strs) = req.membership else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'membership' field".into()),
                    shards: None,
                };
            };
            let Some(s3_src) = req.s3_source else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 's3_source' field".into()),
                    shards: None,
                };
            };
            let s3_config = crate::remote_store::config::S3StorageConfig {
                bucket: s3_src.bucket,
                prefix: s3_src.prefix,
                endpoint_url: s3_src.endpoint,
                region: s3_src.region,
            };
            let Some(snapshot_params) = req.snapshot else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'snapshot' field".into()),
                    shards: None,
                };
            };
            let cfg = ReplicaConfig {
                shard: shard_id,
                listen_addr: listen_addr_str.clone(),
                membership: membership_strs,
            };
            match node.add_writable_clone_from_s3(&cfg, s3_config, snapshot_params).await {
                Ok(()) => AdminResponse {
                    ok: true,
                    message: Some(format!(
                        "writable clone for shard {shard_id} created from S3"
                    )),
                    shards: None,
                },
                Err(e) => AdminResponse {
                    ok: false,
                    message: Some(format!("add_writable_clone_from_s3 failed: {e}")),
                    shards: None,
                },
            }
        }
        "add_read_replica_from_s3" => {
            let Some(shard_id) = req.shard else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'shard' field".into()),
                    shards: None,
                };
            };
            let Some(ref listen_addr_str) = req.listen_addr else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'listen_addr' field".into()),
                    shards: None,
                };
            };
            let listen_addr: std::net::SocketAddr = match listen_addr_str.parse() {
                Ok(a) => a,
                Err(e) => {
                    return AdminResponse {
                        ok: false,
                        message: Some(format!("invalid listen_addr: {e}")),
                        shards: None,
                    };
                }
            };
            let Some(s3_src) = req.s3_source else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 's3_source' field".into()),
                    shards: None,
                };
            };
            let refresh_secs = req.refresh_interval_secs.unwrap_or(30);
            let s3_config = crate::remote_store::config::S3StorageConfig {
                bucket: s3_src.bucket,
                prefix: s3_src.prefix,
                endpoint_url: s3_src.endpoint,
                region: s3_src.region,
            };
            match node
                .add_read_replica_from_s3(
                    ShardNumber(shard_id),
                    listen_addr,
                    s3_config,
                    std::time::Duration::from_secs(refresh_secs),
                )
                .await
            {
                Ok(()) => AdminResponse {
                    ok: true,
                    message: Some(format!("read replica for shard {shard_id} created")),
                    shards: None,
                },
                Err(e) => AdminResponse {
                    ok: false,
                    message: Some(format!("add_read_replica_from_s3 failed: {e}")),
                    shards: None,
                },
            }
        }
        "view_change" => {
            let Some(shard_id) = req.shard else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'shard' field".into()),
                    shards: None,
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
            }
        }
        "remove_replica" => {
            let Some(shard_id) = req.shard else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'shard' field".into()),
                    shards: None,
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
            }
        }
        "leave" => {
            let Some(shard_id) = req.shard else {
                return AdminResponse {
                    ok: false,
                    message: Some("missing 'shard' field".into()),
                    shards: None,
                };
            };
            match node.leave_shard(ShardNumber(shard_id)).await {
                Ok(()) => AdminResponse {
                    ok: true,
                    message: Some(format!("left shard {shard_id}")),
                    shards: None,
                },
                Err(e) => AdminResponse {
                    ok: false,
                    message: Some(format!("leave failed: {e}")),
                    shards: None,
                },
            }
        }
        other => AdminResponse {
            ok: false,
            message: Some(format!("unknown command: {other}")),
            shards: None,
        },
    }
}
