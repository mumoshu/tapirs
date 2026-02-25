use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// Exchange a raw admin request, returning the unparsed response line.
///
/// Used for WaitReady (graceful connection failure) and the generic
/// single-node path (raw JSON pretty-printing).
pub async fn raw_admin_exchange(
    addr: &str,
    request: &str,
    #[cfg(feature = "tls")] tls_connector: &Option<crate::tls::ReloadableTlsConnector>,
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
