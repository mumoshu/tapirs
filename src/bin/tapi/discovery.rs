use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use tapirs::discovery::{
    DiscoveryError, RemoteShardDirectory,
    membership_to_strings, strings_to_membership,
};
use tapirs::{IrMembership, ShardNumber, TcpAddress};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

// ---- HTTP wire format types ----

/// Membership info for a single shard, serialized as JSON for the discovery HTTP API.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardMembership {
    pub id: u32,
    pub replicas: Vec<String>,
    #[serde(default)]
    pub view: u64,
}

/// Full cluster topology returned by the discovery service.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterTopology {
    pub shards: Vec<ShardMembership>,
}

// ---- HttpDiscoveryClient ----

/// HTTP client for the tapi discovery service, implementing [`RemoteShardDirectory<TcpAddress>`].
pub struct HttpDiscoveryClient {
    addr: std::net::SocketAddr,
}

impl HttpDiscoveryClient {
    pub fn new(discovery_url: &str) -> Self {
        let addr_str = discovery_url
            .strip_prefix("http://")
            .unwrap_or(discovery_url);
        let addr: std::net::SocketAddr = addr_str
            .parse()
            .unwrap_or_else(|e| panic!("invalid discovery_url '{discovery_url}': {e}"));
        Self { addr }
    }

    /// Helper to send an HTTP request and return the response body.
    async fn http_request(&self, request: &str) -> Result<(String, String), DiscoveryError> {
        let mut stream = tokio::net::TcpStream::connect(self.addr)
            .await
            .map_err(|e| DiscoveryError::ConnectionFailed(e.to_string()))?;
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| DiscoveryError::ConnectionFailed(e.to_string()))?;
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .map_err(|e| DiscoveryError::ConnectionFailed(e.to_string()))?;
        let resp_str = String::from_utf8_lossy(&response).to_string();
        let body = resp_str
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_string())
            .unwrap_or_default();
        Ok((resp_str, body))
    }
}

impl RemoteShardDirectory<TcpAddress> for HttpDiscoveryClient {
    async fn put(
        &self,
        shard: ShardNumber,
        membership: IrMembership<TcpAddress>,
        view: u64,
    ) -> Result<(), DiscoveryError> {
        let replicas = membership_to_strings(&membership);
        let body = serde_json::to_string(&serde_json::json!({ "replicas": replicas, "view": view }))
            .map_err(|e| DiscoveryError::InvalidResponse(e.to_string()))?;
        let id = shard.0;
        let addr_str = self.addr.to_string();
        let request = format!(
            "POST /v1/shards/{id} HTTP/1.1\r\nHost: {addr_str}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len(),
        );
        let (resp, _) = self.http_request(&request).await?;
        if resp.contains("200 OK") {
            Ok(())
        } else if resp.contains("409") {
            Err(DiscoveryError::Tombstoned)
        } else {
            Err(DiscoveryError::InvalidResponse(resp))
        }
    }

    async fn remove(&self, shard: ShardNumber) -> Result<(), DiscoveryError> {
        let id = shard.0;
        let addr_str = self.addr.to_string();
        let request = format!(
            "DELETE /v1/shards/{id} HTTP/1.1\r\nHost: {addr_str}\r\nConnection: close\r\n\r\n",
        );
        let (resp, _) = self.http_request(&request).await?;
        if resp.contains("200 OK") {
            Ok(())
        } else if resp.contains("404") {
            Err(DiscoveryError::NotFound)
        } else {
            Err(DiscoveryError::InvalidResponse(resp))
        }
    }

    async fn all(
        &self,
    ) -> Result<Vec<(ShardNumber, IrMembership<TcpAddress>, u64)>, DiscoveryError> {
        let addr_str = self.addr.to_string();
        let request = format!(
            "GET /v1/cluster HTTP/1.1\r\nHost: {addr_str}\r\nConnection: close\r\n\r\n",
        );
        let (_, body) = self.http_request(&request).await?;
        let topo: ClusterTopology = serde_json::from_str(&body)
            .map_err(|e| DiscoveryError::InvalidResponse(format!("{e}\nbody: {body}")))?;
        let mut result = Vec::new();
        for s in topo.shards {
            let membership = strings_to_membership::<TcpAddress>(&s.replicas)
                .map_err(DiscoveryError::InvalidResponse)?;
            result.push((ShardNumber(s.id), membership, s.view));
        }
        Ok(result)
    }

    async fn replace(
        &self,
        old: ShardNumber,
        new: ShardNumber,
        membership: IrMembership<TcpAddress>,
        view: u64,
    ) -> Result<(), DiscoveryError> {
        let replicas = membership_to_strings(&membership);
        let body = serde_json::to_string(&serde_json::json!({
            "old_id": old.0,
            "new_id": new.0,
            "replicas": replicas,
            "view": view,
        }))
        .map_err(|e| DiscoveryError::InvalidResponse(e.to_string()))?;
        let addr_str = self.addr.to_string();
        let request = format!(
            "POST /v1/shards/replace HTTP/1.1\r\nHost: {addr_str}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len(),
        );
        let (resp, _) = self.http_request(&request).await?;
        if resp.contains("200 OK") {
            Ok(())
        } else {
            Err(DiscoveryError::InvalidResponse(resp))
        }
    }
}

// ---- Discovery server ----

#[derive(Debug, Deserialize)]
struct RegisterShardRequest {
    replicas: Vec<String>,
    #[serde(default)]
    view: u64,
}

struct DiscoveryState {
    shards: RwLock<HashMap<u32, ShardMembership>>,
    tombstones: RwLock<HashSet<u32>>,
}

fn handle_request(state: &DiscoveryState, method: &str, path: &str, body: &str) -> (u16, String) {
    // POST /v1/shards/replace — atomic swap of old shard for new shard.
    if method == "POST" && path == "/v1/shards/replace" {
        #[derive(Deserialize)]
        struct ReplaceShardRequest {
            old_id: u32,
            new_id: u32,
            replicas: Vec<String>,
            #[serde(default)]
            view: u64,
        }
        let req: ReplaceShardRequest = match serde_json::from_str(body) {
            Ok(r) => r,
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid JSON: {e}"}}"#));
            }
        };
        let mut shards = state.shards.write().unwrap();
        // Tombstone old shard.
        shards.remove(&req.old_id);
        state.tombstones.write().unwrap().insert(req.old_id);
        // Insert new shard with monotonic check.
        if let Some(existing) = shards.get(&req.new_id) {
            if existing.view > req.view {
                tracing::info!(old = req.old_id, new = req.new_id, "shard replaced (new shard stale, kept existing)");
                return (200, r#"{"ok":true}"#.to_string());
            }
        }
        shards.insert(
            req.new_id,
            ShardMembership {
                id: req.new_id,
                replicas: req.replicas,
                view: req.view,
            },
        );
        tracing::info!(old = req.old_id, new = req.new_id, "shard replaced");
        return (200, r#"{"ok":true}"#.to_string());
    }

    // GET /v1/cluster
    if method == "GET" && path == "/v1/cluster" {
        let shards = state.shards.read().unwrap();
        #[allow(clippy::disallowed_methods)] // values() order irrelevant — sorted immediately after
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
                // Reject tombstoned shards.
                if state.tombstones.read().unwrap().contains(&id) {
                    return (409, r#"{"error":"shard tombstoned"}"#.to_string());
                }
                let req: RegisterShardRequest = match serde_json::from_str(body) {
                    Ok(r) => r,
                    Err(e) => {
                        return (400, format!(r#"{{"error":"invalid JSON: {e}"}}"#));
                    }
                };
                let mut shards = state.shards.write().unwrap();
                // Reject stale: if current view > incoming view, no-op.
                if let Some(existing) = shards.get(&id) {
                    if existing.view > req.view {
                        return (200, r#"{"ok":true}"#.to_string());
                    }
                }
                let membership = ShardMembership {
                    id,
                    replicas: req.replicas,
                    view: req.view,
                };
                shards.insert(id, membership);
                tracing::info!(shard = id, view = req.view, "shard registered/updated");
                (200, r#"{"ok":true}"#.to_string())
            }
            "DELETE" => {
                let mut shards = state.shards.write().unwrap();
                let removed = shards.remove(&id);
                // Tombstone the shard (even if already tombstoned, idempotent).
                state.tombstones.write().unwrap().insert(id);
                if removed.is_some() {
                    tracing::info!(shard = id, "shard deregistered (tombstoned)");
                    (200, r#"{"ok":true}"#.to_string())
                } else {
                    // Return 200 even if already tombstoned — idempotent.
                    (200, r#"{"ok":true}"#.to_string())
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
        409 => "Conflict",
        _ => "Unknown",
    }
}

pub(crate) async fn serve(listener: TcpListener) {
    let state = Arc::new(DiscoveryState {
        shards: RwLock::new(HashMap::new()),
        tombstones: RwLock::new(HashSet::new()),
    });

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

pub async fn run(listen_addr: String) {
    let addr: std::net::SocketAddr = listen_addr
        .parse()
        .unwrap_or_else(|e| panic!("invalid discovery listen address '{listen_addr}': {e}"));

    let listener = TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("discovery: failed to bind {addr}: {e}"));

    tracing::info!(%addr, "discovery server starting");

    serve(listener).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> DiscoveryState {
        DiscoveryState {
            shards: RwLock::new(HashMap::new()),
            tombstones: RwLock::new(HashSet::new()),
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
        // DELETE always returns 200 (idempotent tombstoning).
        let (code, _) = handle_request(&s, "DELETE", "/v1/shards/99", "");
        assert_eq!(code, 200);
    }

    #[test]
    fn replace_shard_atomic() {
        let s = state();
        // Register shard 1 and an unrelated shard 5.
        handle_request(
            &s,
            "POST",
            "/v1/shards/1",
            r#"{"replicas":["127.0.0.1:5001","127.0.0.1:5002"]}"#,
        );
        handle_request(
            &s,
            "POST",
            "/v1/shards/5",
            r#"{"replicas":["127.0.0.1:9001"]}"#,
        );

        // Replace shard 1 with shard 3.
        let (code, _) = handle_request(
            &s,
            "POST",
            "/v1/shards/replace",
            r#"{"old_id":1,"new_id":3,"replicas":["127.0.0.1:7001","127.0.0.1:7002"]}"#,
        );
        assert_eq!(code, 200);

        // Old shard gone.
        let (code, _) = handle_request(&s, "GET", "/v1/shards/1", "");
        assert_eq!(code, 404);

        // New shard present with correct replicas.
        let (code, body) = handle_request(&s, "GET", "/v1/shards/3", "");
        assert_eq!(code, 200);
        let m: ShardMembership = serde_json::from_str(&body).unwrap();
        assert_eq!(m.id, 3);
        assert_eq!(m.replicas, vec!["127.0.0.1:7001", "127.0.0.1:7002"]);

        // Unrelated shard untouched.
        let (code, _) = handle_request(&s, "GET", "/v1/shards/5", "");
        assert_eq!(code, 200);
    }

    /// Start a discovery server on an ephemeral port and return the address.
    async fn start_test_server() -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener));
        addr
    }

    #[tokio::test]
    async fn http_client_register_and_topology() {
        let addr = start_test_server().await;
        let client = HttpDiscoveryClient::new(&addr.to_string());
        let m = |a: &str| TcpAddress(a.parse().unwrap());

        // Register two shards.
        client
            .put(
                ShardNumber(1),
                IrMembership::new(vec![m("127.0.0.1:5001"), m("127.0.0.1:5002")]),
                0,
            )
            .await
            .unwrap();
        client
            .put(
                ShardNumber(2),
                IrMembership::new(vec![m("127.0.0.1:6001")]),
                0,
            )
            .await
            .unwrap();

        // Query full topology.
        let entries = client.all().await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, ShardNumber(1));
        assert_eq!(entries[0].1.len(), 2);
        assert_eq!(entries[1].0, ShardNumber(2));
        assert_eq!(entries[1].1.len(), 1);
    }

    #[tokio::test]
    async fn http_client_deregister() {
        let addr = start_test_server().await;
        let client = HttpDiscoveryClient::new(&addr.to_string());
        let m = |a: &str| TcpAddress(a.parse().unwrap());

        client
            .put(
                ShardNumber(1),
                IrMembership::new(vec![m("127.0.0.1:5001")]),
                0,
            )
            .await
            .unwrap();
        client.remove(ShardNumber(1)).await.unwrap();

        let entries = client.all().await.unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn http_client_deregister_nonexistent() {
        let addr = start_test_server().await;
        let client = HttpDiscoveryClient::new(&addr.to_string());

        // DELETE is idempotent — always returns 200 (tombstones the shard).
        let result = client.remove(ShardNumber(99)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn http_client_replace_shard() {
        let addr = start_test_server().await;
        let client = HttpDiscoveryClient::new(&addr.to_string());
        let m = |a: &str| TcpAddress(a.parse().unwrap());

        // Register shard 1.
        client
            .put(
                ShardNumber(1),
                IrMembership::new(vec![m("127.0.0.1:5001"), m("127.0.0.1:5002")]),
                0,
            )
            .await
            .unwrap();

        // Replace shard 1 with shard 3.
        client
            .replace(
                ShardNumber(1),
                ShardNumber(3),
                IrMembership::new(vec![m("127.0.0.1:7001"), m("127.0.0.1:7002")]),
                0,
            )
            .await
            .unwrap();

        // Topology should have shard 3, not shard 1.
        let entries = client.all().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, ShardNumber(3));
        assert_eq!(entries[0].1.len(), 2);
    }

    #[tokio::test]
    async fn http_client_connection_failure() {
        // Point at a port where nothing is listening.
        let client = HttpDiscoveryClient::new("127.0.0.1:1");

        let result = client.all().await;
        assert!(matches!(result, Err(DiscoveryError::ConnectionFailed(_))));
    }

    #[tokio::test]
    async fn caching_directory_push_pull_with_http() {
        use tapirs::discovery::{
            CachingShardDirectory, InMemoryShardDirectory, ShardDirectory as _,
        };

        let addr = start_test_server().await;
        let directory = Arc::new(InMemoryShardDirectory::new());
        let client = Arc::new(HttpDiscoveryClient::new(&addr.to_string()));
        let dir = CachingShardDirectory::<TcpAddress, _>::new(
            Arc::clone(&directory),
            Arc::clone(&client),
            std::time::Duration::from_millis(50),
        );

        // PUSH: put membership locally and register as own shard.
        dir.add_own_shard(ShardNumber(1));
        let membership = IrMembership::new(vec![
            TcpAddress("127.0.0.1:5001".parse().unwrap()),
            TcpAddress("127.0.0.1:5002".parse().unwrap()),
        ]);
        dir.put(ShardNumber(1), membership, 0);

        // Wait for background sync to push.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Verify it was pushed to discovery.
        let entries = client.all().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, ShardNumber(1));

        // PULL: register another shard directly on discovery (simulates another node).
        let m = |a: &str| TcpAddress(a.parse().unwrap());
        client
            .put(
                ShardNumber(2),
                IrMembership::new(vec![m("127.0.0.1:7001"), m("127.0.0.1:7002")]),
                0,
            )
            .await
            .unwrap();

        // Wait for background sync to pull.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Verify it was pulled into the local directory.
        let (pulled, _) = dir.get(ShardNumber(2)).unwrap();
        assert_eq!(pulled.len(), 2);
    }

    #[tokio::test]
    async fn caching_directory_resilience() {
        use tapirs::discovery::{
            CachingShardDirectory, InMemoryShardDirectory, ShardDirectory as _,
        };

        // Point at a non-existent server.
        let directory = Arc::new(InMemoryShardDirectory::new());
        let client = Arc::new(HttpDiscoveryClient::new("127.0.0.1:1"));
        let dir = CachingShardDirectory::<TcpAddress, _>::new(
            Arc::clone(&directory),
            client,
            std::time::Duration::from_millis(50),
        );

        // Local operations succeed despite remote being unavailable.
        let membership = IrMembership::new(vec![
            TcpAddress("127.0.0.1:5001".parse().unwrap()),
        ]);
        dir.put(ShardNumber(1), membership, 0);

        let (got, _) = dir.get(ShardNumber(1)).unwrap();
        assert_eq!(got.len(), 1);

        // Background sync fails silently — no panic.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Local data is unaffected.
        let (got, _) = dir.get(ShardNumber(1)).unwrap();
        assert_eq!(got.len(), 1);
    }
}
