use crate::discovery::{RemoteShardDirectory, ShardStatus, strings_to_membership};
use crate::{IrMembership, KeyRange, ShardNumber, TcpAddress};
use serde::Deserialize;
use super::{ShardManagerState, JoinRequest};

pub(crate) async fn handle_request<RD: RemoteShardDirectory<TcpAddress, String>>(
    state: &ShardManagerState<RD>,
    method: &str,
    path: &str,
    body: &str,
) -> (u16, String) {
    if method == "GET" && path == "/healthz" {
        tracing::debug!("healthz: checking discovery connectivity");
        match tokio::time::timeout(std::time::Duration::from_secs(3), state.remote.weak_all_active_shard_view_memberships()).await {
            Ok(Ok(_)) => {
                return (200, r#"{"ok":true}"#.to_string());
            }
            Ok(Err(e)) => {
                return (
                    503,
                    format!(r#"{{"ok":false,"error":"discovery error: {e}"}}"#),
                );
            }
            Err(_) => {
                return (
                    503,
                    r#"{"ok":false,"error":"discovery timeout"}"#.to_string(),
                );
            }
        }
    }

    if method == "POST" && path == "/v1/join" {
        let req: JoinRequest = match serde_json::from_str(body) {
            Ok(r) => r,
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid JSON: {e}"}}"#));
            }
        };
        let shard = ShardNumber(req.shard);
        let new_addr: TcpAddress = match req.listen_addr.parse() {
            Ok(a) => TcpAddress(a),
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid listen_addr: {e}"}}"#));
            }
        };

        // Strong read: check shard exists and is Active before calling join().
        let exists = state.remote.strong_get_shard(shard).await
            .ok().flatten()
            .is_some_and(|r| r.status == ShardStatus::Active && r.membership.len() > 0);

        if exists {
            match state.manager.lock().await.join(shard, new_addr).await {
                Ok(()) => (200, r#"{"ok":true}"#.to_string()),
                Err(e) => {
                    (
                        500,
                        format!(r#"{{"error":"join failed: {e}"}}"#),
                    )
                }
            }
        } else {
            // Shard not found in discovery — refuse to auto-bootstrap.
            // Initial shard creation must use static membership (--membership
            // flag in add-replica). The /v1/join path is only for adding a
            // replica to an existing shard already visible in discovery.
            (
                400,
                format!(r#"{{"error":"shard {} not found in discovery"}}"#, req.shard),
            )
        }
    } else if method == "POST" && path == "/v1/leave" {
        let req: JoinRequest = match serde_json::from_str(body) {
            Ok(r) => r,
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid JSON: {e}"}}"#));
            }
        };
        let shard = ShardNumber(req.shard);
        let addr: TcpAddress = match req.listen_addr.parse() {
            Ok(a) => TcpAddress(a),
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid listen_addr: {e}"}}"#));
            }
        };

        // Strong read: check shard exists and address is in membership.
        eprintln!("[leave-handler] querying discovery for shard={shard:?} to remove addr={addr:?}");
        let record = state.remote.strong_get_shard(shard).await
            .ok().flatten()
            .filter(|r| r.status == ShardStatus::Active && r.membership.len() > 0);

        let Some(record) = record else {
            eprintln!("[leave-handler] shard {shard:?} not found in discovery");
            return (
                400,
                format!(r#"{{"error":"shard {} not found in discovery"}}"#, req.shard),
            );
        };
        eprintln!("[leave-handler] discovery returned membership len={} view={} for shard={shard:?}", record.membership.len(), record.view);

        // Verify the address is part of the shard.
        if !record.membership.contains(addr) {
            eprintln!("[leave-handler] addr={addr:?} not in membership for shard={shard:?}");
            return (
                400,
                format!(
                    r#"{{"error":"address {} not in shard {}"}}"#,
                    req.listen_addr, req.shard
                ),
            );
        }

        eprintln!("[leave-handler] calling manager.leave(shard={shard:?}, addr={addr:?})");
        match state.manager.lock().await.leave(shard, addr).await {
            Ok(()) => (200, r#"{"ok":true}"#.to_string()),
            Err(e) => (500, format!(r#"{{"error":"leave failed: {e}"}}"#)),
        }
    } else if method == "POST" && path == "/v1/register" {
        #[derive(Deserialize)]
        struct RegisterRequest {
            shard: u32,
            key_range_start: Option<String>,
            key_range_end: Option<String>,
            #[serde(default)]
            replicas: Option<Vec<String>>,
        }
        let req: RegisterRequest = match serde_json::from_str(body) {
            Ok(r) => r,
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid JSON: {e}"}}"#));
            }
        };
        let shard = ShardNumber(req.shard);
        tracing::info!(?shard, "register: received request");

        // Use provided replicas or query discovery for shard membership.
        let (membership, _view) = if let Some(ref replicas) = req.replicas {
            tracing::info!(?shard, replicas_count = replicas.len(), "register: using provided replicas");
            match strings_to_membership::<TcpAddress>(replicas) {
                Ok(m) => (m, 0u64),
                Err(e) => {
                    return (400, format!(r#"{{"error":"invalid replicas: {e}"}}"#));
                }
            }
        } else {
            tracing::info!(?shard, "register: querying discovery for membership");
            let existing = state.remote.strong_get_shard(shard).await
                .ok().flatten()
                .filter(|r| r.status == ShardStatus::Active)
                .map(|r| (r.membership, r.view));

            match existing {
                Some((m, v)) => (m, v),
                None => {
                    return (
                        400,
                        format!(
                            r#"{{"error":"shard {} not found in discovery (provide replicas in request or populate discovery first)"}}"#,
                            req.shard
                        ),
                    );
                }
            }
        };

        let key_range = KeyRange {
            start: req.key_range_start,
            end: req.key_range_end,
        };
        tracing::info!(?shard, ?key_range, "register: acquiring manager lock");
        let manager = state.manager.lock().await;
        tracing::info!(?shard, "register: calling register_active_shard (strong_atomic_update_shards)");
        if let Err(e) = manager.register_active_shard(shard, membership, key_range).await {
            drop(manager);
            tracing::error!(?shard, %e, "register: failed");
            return (500, format!(r#"{{"ok":false,"error":"registration failed: {e}"}}"#));
        }
        drop(manager);
        tracing::info!(?shard, "register: completed successfully");
        (200, r#"{"ok":true}"#.to_string())
    } else if method == "POST" && path == "/v1/split" {
        #[derive(Deserialize)]
        struct SplitRequest {
            source: u32,
            split_key: String,
            new_shard: u32,
            new_replicas: Vec<String>,
        }
        let req: SplitRequest = match serde_json::from_str(body) {
            Ok(r) => r,
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid JSON: {e}"}}"#));
            }
        };

        let new_membership: IrMembership<TcpAddress> =
            match strings_to_membership(&req.new_replicas) {
                Ok(m) => m,
                Err(e) => {
                    return (400, format!(r#"{{"error":"invalid new_replicas: {e}"}}"#));
                }
            };

        match state
            .manager
            .lock()
            .await
            .split(
                ShardNumber(req.source),
                req.split_key,
                ShardNumber(req.new_shard),
                new_membership,
            )
            .await
        {
            Ok(()) => (200, r#"{"ok":true}"#.to_string()),
            Err(e) => (500, format!(r#"{{"error":"split failed: {e:?}"}}"#)),
        }
    } else if method == "POST" && path == "/v1/merge" {
        #[derive(Deserialize)]
        struct MergeRequest {
            absorbed: u32,
            surviving: u32,
        }
        let req: MergeRequest = match serde_json::from_str(body) {
            Ok(r) => r,
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid JSON: {e}"}}"#));
            }
        };

        match state
            .manager
            .lock()
            .await
            .merge(ShardNumber(req.absorbed), ShardNumber(req.surviving))
            .await
        {
            Ok(()) => (200, r#"{"ok":true}"#.to_string()),
            Err(e) => (500, format!(r#"{{"error":"merge failed: {e:?}"}}"#)),
        }
    } else if method == "POST" && path == "/v1/compact" {
        #[derive(Deserialize)]
        struct CompactRequest {
            source: u32,
            new_shard: u32,
            new_replicas: Vec<String>,
        }
        let req: CompactRequest = match serde_json::from_str(body) {
            Ok(r) => r,
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid JSON: {e}"}}"#));
            }
        };

        let new_membership: IrMembership<TcpAddress> =
            match strings_to_membership(&req.new_replicas) {
                Ok(m) => m,
                Err(e) => {
                    return (400, format!(r#"{{"error":"invalid new_replicas: {e}"}}"#));
                }
            };

        match state
            .manager
            .lock()
            .await
            .compact(
                ShardNumber(req.source),
                ShardNumber(req.new_shard),
                new_membership,
            )
            .await
        {
            Ok(()) => (200, r#"{"ok":true}"#.to_string()),
            Err(e) => (500, format!(r#"{{"error":"compact failed: {e:?}"}}"#)),
        }
    } else {
        (404, r#"{"error":"not found"}"#.to_string())
    }
}
