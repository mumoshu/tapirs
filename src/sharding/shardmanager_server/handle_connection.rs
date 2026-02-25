use crate::discovery::RemoteShardDirectory;
use crate::TcpAddress;
use super::{ShardManagerState, status_text};
use super::handle_request::handle_request;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

pub(crate) async fn handle_connection<R, W, RD>(reader: R, mut writer: W, state: &ShardManagerState<RD>)
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
    RD: RemoteShardDirectory<TcpAddress, String>,
{
    let mut buf_reader = BufReader::new(reader);

    // Read request line.
    let mut request_line = String::new();
    if buf_reader.read_line(&mut request_line).await.is_err() {
        return;
    }
    let parts: Vec<&str> = request_line.trim().splitn(3, ' ').collect();
    if parts.len() < 2 {
        return;
    }
    let method = parts[0];
    let path = parts[1];

    // Read headers to find Content-Length.
    let mut content_length: usize = 0;
    loop {
        let mut header_line = String::new();
        if buf_reader.read_line(&mut header_line).await.is_err() {
            return;
        }
        let trimmed = header_line.trim();
        if trimmed.is_empty() {
            break;
        }
        if let Some(val) = trimmed.strip_prefix("Content-Length:")
            && let Ok(len) = val.trim().parse::<usize>()
        {
            content_length = len;
        }
        if let Some(val) = trimmed.strip_prefix("content-length:")
            && let Ok(len) = val.trim().parse::<usize>()
        {
            content_length = len;
        }
    }

    // Read body if present.
    let body = if content_length > 0 {
        let mut buf = vec![0u8; content_length];
        if buf_reader.read_exact(&mut buf).await.is_err() {
            return;
        }
        String::from_utf8_lossy(&buf).to_string()
    } else {
        String::new()
    };

    let (status, response_body) = match tokio::time::timeout(
        std::time::Duration::from_secs(60),
        handle_request(state, method, path, &body),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            tracing::warn!(method, path, "request timed out after 60s");
            (504, r#"{"error":"request timed out"}"#.to_string())
        }
    };

    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        status_text(status),
        response_body.len(),
        response_body,
    );
    let _ = writer.write_all(response.as_bytes()).await;
    // Flush and send TLS close_notify (if TLS). Without this, rustls clients
    // see "peer closed connection without sending TLS close_notify" on read_to_end.
    let _ = writer.shutdown().await;
}
