mod handle_request;
mod start;

pub(crate) use self::start::start;

use super::Node;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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
    backup: Option<super::ShardBackup>,
    #[serde(default)]
    new_membership: Option<Vec<String>>,
    /// Static membership for add_replica. When provided, creates the replica
    /// with the specified membership directly (no shard-manager involvement).
    /// When absent, uses add_replica_join() which coordinates via shard-manager.
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
    backup: Option<super::ShardBackup>,
}

#[derive(Serialize)]
struct ShardInfo {
    shard: u32,
    listen_addr: String,
}

async fn handle_admin_connection<R, W>(reader: R, mut writer: W, node: &Node)
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let resp = handle_request::handle_request(node, &line).await;
        let mut out = serde_json::to_string(&resp).unwrap();
        out.push('\n');
        let _ = writer.write_all(out.as_bytes()).await;
    }
}
