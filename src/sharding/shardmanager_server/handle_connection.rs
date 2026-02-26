// HashMap used for lookup-only data (no iteration affecting execution order).
#![allow(clippy::disallowed_types)]

use std::collections::HashMap;
use crate::discovery::RemoteShardDirectory;
use crate::TcpAddress;
use super::{ShardManagerState, status_text};
use super::handle_apply_changes::handle_apply_changes;
use super::handle_request::handle_request;
use super::handle_scan_changes::handle_scan_changes;
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

    // Read headers into a map, tracking Content-Length.
    let mut content_length: usize = 0;
    let mut headers = HashMap::new();
    loop {
        let mut header_line = String::new();
        if buf_reader.read_line(&mut header_line).await.is_err() {
            return;
        }
        let trimmed = header_line.trim();
        if trimmed.is_empty() {
            break;
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            let name_lower = name.trim().to_ascii_lowercase();
            let value_trimmed = value.trim().to_string();
            if name_lower == "content-length"
                && let Ok(len) = value_trimmed.parse::<usize>()
            {
                content_length = len;
            }
            headers.insert(name_lower, value_trimmed);
        }
    }

    // Read body as raw bytes.
    let body_bytes = if content_length > 0 {
        let mut buf = vec![0u8; content_length];
        if buf_reader.read_exact(&mut buf).await.is_err() {
            return;
        }
        buf
    } else {
        Vec::new()
    };

    // Dispatch binary endpoints.
    if method == "POST" && path == "/v1/scan-changes" {
        let (status, response_bytes) = match tokio::time::timeout(
            std::time::Duration::from_secs(60),
            handle_scan_changes(state, &body_bytes),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                tracing::warn!(method, path, "request timed out after 60s");
                (504, r#"{"error":"request timed out"}"#.as_bytes().to_vec())
            }
        };

        // Determine content type from status: 200 = binary, else JSON error.
        let content_type = if status == 200 { "application/octet-stream" } else { "application/json" };
        let header = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            status,
            status_text(status),
            content_type,
            response_bytes.len(),
        );
        let _ = writer.write_all(header.as_bytes()).await;
        let _ = writer.write_all(&response_bytes).await;
        let _ = writer.shutdown().await;
        return;
    }

    if method == "POST" && path == "/v1/apply-changes" {
        let (status, response_body) = match tokio::time::timeout(
            std::time::Duration::from_secs(60),
            handle_apply_changes(state, &headers, &body_bytes),
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
        let _ = writer.shutdown().await;
        return;
    }

    // Fall through to existing JSON handler.
    let body = String::from_utf8_lossy(&body_bytes).to_string();
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
