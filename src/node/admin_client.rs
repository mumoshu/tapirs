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
    send_admin_request_inner(
        addr,
        request_json,
        #[cfg(feature = "tls")]
        None,
    )
    .await
}

#[cfg(feature = "tls")]
pub async fn send_admin_request_tls(
    addr: &str,
    request_json: &str,
    tls_connector: &crate::tls::ReloadableTlsConnector,
) -> Result<AdminStatusResponse, String> {
    send_admin_request_inner(addr, request_json, Some(tls_connector)).await
}

async fn send_admin_request_inner(
    addr: &str,
    request_json: &str,
    #[cfg(feature = "tls")] tls_connector: Option<&crate::tls::ReloadableTlsConnector>,
) -> Result<AdminStatusResponse, String> {
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connect to {addr}: {e}"))?;

    #[cfg(feature = "tls")]
    if let Some(connector) = tls_connector {
        let tls_connector = connector.connector();
        let host = addr.split(':').next().unwrap_or(addr).to_string();
        let server_name = rustls::pki_types::ServerName::try_from(host)
            .map_err(|e| format!("invalid server name: {e}"))?;
        let tls_stream = tls_connector
            .connect(server_name, stream)
            .await
            .map_err(|e| format!("TLS connect to {addr}: {e}"))?;
        let (reader, writer) = tokio::io::split(tls_stream);
        return send_and_recv(addr, request_json, reader, writer).await;
    }

    let (reader, writer) = stream.into_split();
    send_and_recv(addr, request_json, reader, writer).await
}

async fn send_and_recv<R, W>(
    addr: &str,
    request_json: &str,
    reader: R,
    mut writer: W,
) -> Result<AdminStatusResponse, String>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
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
