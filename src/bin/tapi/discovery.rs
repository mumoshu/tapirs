use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardMembership {
    pub id: u32,
    pub replicas: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterTopology {
    pub shards: Vec<ShardMembership>,
}

#[derive(Debug, Deserialize)]
struct RegisterShardRequest {
    replicas: Vec<String>,
}

struct DiscoveryState {
    shards: RwLock<HashMap<u32, ShardMembership>>,
}

fn handle_request(state: &DiscoveryState, method: &str, path: &str, body: &str) -> (u16, String) {
    // GET /v1/cluster
    if method == "GET" && path == "/v1/cluster" {
        let shards = state.shards.read().unwrap();
        let mut entries: Vec<ShardMembership> = shards.values().cloned().collect();
        entries.sort_by_key(|s| s.id);
        let topology = ClusterTopology { shards: entries };
        return (200, serde_json::to_string(&topology).unwrap());
    }

    // Routes under /v1/shards/{id}
    if let Some(id_str) = path.strip_prefix("/v1/shards/") {
        let Ok(id) = id_str.parse::<u32>() else {
            return (400, r#"{"error":"invalid shard id"}"#.to_string());
        };

        match method {
            "GET" => {
                let shards = state.shards.read().unwrap();
                match shards.get(&id) {
                    Some(m) => (200, serde_json::to_string(m).unwrap()),
                    None => (404, r#"{"error":"shard not found"}"#.to_string()),
                }
            }
            "POST" => {
                let req: RegisterShardRequest = match serde_json::from_str(body) {
                    Ok(r) => r,
                    Err(e) => {
                        return (400, format!(r#"{{"error":"invalid JSON: {e}"}}"#));
                    }
                };
                let membership = ShardMembership {
                    id,
                    replicas: req.replicas,
                };
                state.shards.write().unwrap().insert(id, membership);
                tracing::info!(shard = id, "shard registered/updated");
                (200, r#"{"ok":true}"#.to_string())
            }
            "DELETE" => {
                let removed = state.shards.write().unwrap().remove(&id);
                if removed.is_some() {
                    tracing::info!(shard = id, "shard deregistered");
                    (200, r#"{"ok":true}"#.to_string())
                } else {
                    (404, r#"{"error":"shard not found"}"#.to_string())
                }
            }
            _ => (405, r#"{"error":"method not allowed"}"#.to_string()),
        }
    } else {
        (404, r#"{"error":"not found"}"#.to_string())
    }
}

fn status_text(code: u16) -> &'static str {
    match code {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Unknown",
    }
}

pub async fn run(listen_addr: String) {
    let state = Arc::new(DiscoveryState {
        shards: RwLock::new(HashMap::new()),
    });

    let addr: std::net::SocketAddr = listen_addr
        .parse()
        .unwrap_or_else(|e| panic!("invalid discovery listen address '{listen_addr}': {e}"));

    let listener = TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("discovery: failed to bind {addr}: {e}"));

    tracing::info!(%addr, "discovery server starting");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    let (reader, mut writer) = stream.into_split();
                    let mut buf_reader = BufReader::new(reader);

                    // Read request line: "METHOD /path HTTP/1.1\r\n"
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
                            break; // End of headers.
                        }
                        if let Some(val) = trimmed.strip_prefix("Content-Length:") {
                            if let Ok(len) = val.trim().parse::<usize>() {
                                content_length = len;
                            }
                        }
                        // Also accept lowercase per HTTP spec.
                        if let Some(val) = trimmed.strip_prefix("content-length:") {
                            if let Ok(len) = val.trim().parse::<usize>() {
                                content_length = len;
                            }
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

                    let (status, response_body) = handle_request(&state, method, path, &body);

                    let response = format!(
                        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        status,
                        status_text(status),
                        response_body.len(),
                        response_body,
                    );
                    let _ = writer.write_all(response.as_bytes()).await;
                });
            }
            Err(e) => {
                tracing::warn!("discovery accept error: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> DiscoveryState {
        DiscoveryState {
            shards: RwLock::new(HashMap::new()),
        }
    }

    #[test]
    fn empty_cluster() {
        let s = state();
        let (code, body) = handle_request(&s, "GET", "/v1/cluster", "");
        assert_eq!(code, 200);
        let topo: ClusterTopology = serde_json::from_str(&body).unwrap();
        assert!(topo.shards.is_empty());
    }

    #[test]
    fn register_then_query() {
        let s = state();
        let (code, _) = handle_request(
            &s,
            "POST",
            "/v1/shards/1",
            r#"{"replicas":["127.0.0.1:5001","127.0.0.1:5002"]}"#,
        );
        assert_eq!(code, 200);

        let (code, body) = handle_request(&s, "GET", "/v1/shards/1", "");
        assert_eq!(code, 200);
        let m: ShardMembership = serde_json::from_str(&body).unwrap();
        assert_eq!(m.id, 1);
        assert_eq!(m.replicas, vec!["127.0.0.1:5001", "127.0.0.1:5002"]);
    }

    #[test]
    fn query_nonexistent_shard() {
        let s = state();
        let (code, _) = handle_request(&s, "GET", "/v1/shards/99", "");
        assert_eq!(code, 404);
    }

    #[test]
    fn delete_shard() {
        let s = state();
        handle_request(
            &s,
            "POST",
            "/v1/shards/2",
            r#"{"replicas":["127.0.0.1:6000"]}"#,
        );

        let (code, _) = handle_request(&s, "DELETE", "/v1/shards/2", "");
        assert_eq!(code, 200);

        let (code, _) = handle_request(&s, "GET", "/v1/shards/2", "");
        assert_eq!(code, 404);
    }

    #[test]
    fn register_overwrites() {
        let s = state();
        handle_request(
            &s,
            "POST",
            "/v1/shards/1",
            r#"{"replicas":["127.0.0.1:5001"]}"#,
        );
        handle_request(
            &s,
            "POST",
            "/v1/shards/1",
            r#"{"replicas":["127.0.0.1:6001","127.0.0.1:6002"]}"#,
        );

        let (code, body) = handle_request(&s, "GET", "/v1/shards/1", "");
        assert_eq!(code, 200);
        let m: ShardMembership = serde_json::from_str(&body).unwrap();
        assert_eq!(m.replicas, vec!["127.0.0.1:6001", "127.0.0.1:6002"]);
    }

    #[test]
    fn cluster_returns_sorted() {
        let s = state();
        handle_request(
            &s,
            "POST",
            "/v1/shards/3",
            r#"{"replicas":["127.0.0.1:9000"]}"#,
        );
        handle_request(
            &s,
            "POST",
            "/v1/shards/1",
            r#"{"replicas":["127.0.0.1:7000"]}"#,
        );

        let (code, body) = handle_request(&s, "GET", "/v1/cluster", "");
        assert_eq!(code, 200);
        let topo: ClusterTopology = serde_json::from_str(&body).unwrap();
        assert_eq!(topo.shards.len(), 2);
        assert_eq!(topo.shards[0].id, 1);
        assert_eq!(topo.shards[1].id, 3);
    }

    #[test]
    fn delete_nonexistent() {
        let s = state();
        let (code, _) = handle_request(&s, "DELETE", "/v1/shards/99", "");
        assert_eq!(code, 404);
    }
}
