use crate::node::ShardBackup;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[derive(Deserialize)]
pub struct AdminStatusResponse {
    pub ok: bool,
    pub message: Option<String>,
    pub shards: Option<Vec<ShardInfoResponse>>,
    pub backup: Option<ShardBackup>,
}

#[derive(Deserialize)]
pub struct ShardInfoResponse {
    pub shard: u32,
    pub listen_addr: String,
}

pub async fn send_admin_request(
    addr: &str,
    request_json: &str,
) -> Result<AdminStatusResponse, String> {
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
