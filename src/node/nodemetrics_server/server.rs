use super::render::render_metrics;
use super::MetricsCollector;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

/// Start a Prometheus metrics HTTP server.
///
/// Spawns a background task that listens on `addr` and serves `/metrics`
/// in Prometheus exposition format. The server collects metrics from the
/// given `collector` on each scrape.
pub async fn start<T: MetricsCollector>(addr: std::net::SocketAddr, collector: Arc<T>) {
    let listener = TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("metrics: failed to bind {addr}: {e}"));

    tracing::info!(%addr, "metrics server starting");

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let collector = Arc::clone(&collector);
                    tokio::spawn(async move {
                        let (reader, mut writer) = stream.into_split();
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

                        // Drain remaining headers (we don't need them).
                        loop {
                            let mut header_line = String::new();
                            if buf_reader.read_line(&mut header_line).await.is_err() {
                                return;
                            }
                            if header_line.trim().is_empty() {
                                break;
                            }
                        }

                        let (status, content_type, body) = if method == "GET" && path == "/metrics"
                        {
                            let metrics = collector.collect_metrics();
                            let body = render_metrics(&metrics);
                            (200, "text/plain; version=0.0.4; charset=utf-8", body)
                        } else if method == "GET" && path == "/readyz" {
                            let metrics = collector.collect_metrics();
                            let all_ready = !metrics.is_empty()
                                && metrics
                                    .iter()
                                    .all(|(_, m)| m.view_change_count >= 1);
                            if all_ready {
                                (200, "application/json", r#"{"ok":true}"#.to_string())
                            } else {
                                (503, "application/json", r#"{"ok":false}"#.to_string())
                            }
                        } else {
                            (404, "text/plain", "Not Found\n".to_string())
                        };

                        let status_text = match status {
                            200 => "OK",
                            503 => "Service Unavailable",
                            _ => "Not Found",
                        };

                        let response = format!(
                            "HTTP/1.1 {status} {status_text}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len(),
                        );
                        let _ = writer.write_all(response.as_bytes()).await;
                    });
                }
                Err(e) => {
                    tracing::warn!("metrics accept error: {e}");
                }
            }
        }
    });
}
