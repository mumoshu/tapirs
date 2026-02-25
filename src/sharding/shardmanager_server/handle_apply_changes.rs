use crate::discovery::{RemoteShardDirectory, strings_to_membership};
use crate::tapir::LeaderRecordDelta;
use crate::{ShardNumber, TcpAddress};
use std::collections::HashMap;
use super::ShardManagerState;

pub(crate) async fn handle_apply_changes<RD: RemoteShardDirectory<TcpAddress, String>>(
    state: &ShardManagerState<RD>,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> (u16, String) {
    // Parse shard number from X-Shard header.
    let shard_str = match headers.get("x-shard") {
        Some(s) => s,
        None => return (400, r#"{"error":"missing X-Shard header"}"#.to_string()),
    };
    let shard_num: u32 = match shard_str.trim().parse() {
        Ok(n) => n,
        Err(e) => return (400, format!(r#"{{"error":"invalid X-Shard: {e}"}}"#)),
    };

    // Parse replicas from X-Replicas header.
    let replicas_str = match headers.get("x-replicas") {
        Some(s) => s,
        None => return (400, r#"{"error":"missing X-Replicas header"}"#.to_string()),
    };
    let replica_addrs: Vec<String> = replicas_str.split(',').map(|s| s.trim().to_string()).collect();
    let membership = match strings_to_membership::<TcpAddress>(&replica_addrs) {
        Ok(m) => m,
        Err(e) => return (400, format!(r#"{{"error":"invalid X-Replicas: {e}"}}"#)),
    };

    // Deserialize binary body as Vec<LeaderRecordDelta>.
    let deltas: Vec<LeaderRecordDelta<String, String>> = match bitcode::deserialize(body) {
        Ok(d) => d,
        Err(e) => return (400, format!(r#"{{"error":"invalid body: {e}"}}"#)),
    };

    let shard = ShardNumber(shard_num);
    state.manager.lock().await.apply_changes(shard, membership, &deltas).await;
    (200, r#"{"ok":true}"#.to_string())
}
