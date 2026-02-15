use crate::AdminAction;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

pub async fn run(action: AdminAction) {
    let (addr, request) = match action {
        AdminAction::Status { admin_listen_addr } => (
            admin_listen_addr,
            r#"{"command":"status"}"#.to_string(),
        ),
        AdminAction::AddReplica {
            admin_listen_addr,
            shard,
        } => (
            admin_listen_addr,
            format!(r#"{{"command":"add_replica","shard":{shard}}}"#),
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
