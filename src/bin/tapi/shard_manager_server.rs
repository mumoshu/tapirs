use crate::discovery::HttpDiscoveryClient;
use rand::{thread_rng, Rng as _};
use serde::Deserialize;
use std::sync::Arc;
use tapirs::discovery::{DiscoveryClient as _, InMemoryShardDirectory, ShardDirectory as _};
use tapirs::{IrMembership, ShardManager, ShardNumber, TapirReplica, TcpAddress, TcpTransport};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

type TapirShardManager = ShardManager<
    String,
    String,
    TcpTransport<TapirReplica<String, String>>,
    Arc<InMemoryShardDirectory<TcpAddress>>,
>;

struct ShardManagerState {
    manager: tokio::sync::Mutex<TapirShardManager>,
    directory: Arc<InMemoryShardDirectory<TcpAddress>>,
    discovery_client: Arc<HttpDiscoveryClient>,
}

impl ShardManagerState {
    fn new(discovery_url: &str) -> Self {
        let ephemeral_addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
            let a = l.local_addr().unwrap();
            drop(l);
            TcpAddress(a)
        };
        let directory = Arc::new(InMemoryShardDirectory::new());
        let persist_dir = format!("/tmp/tapi_shard_manager_{}", std::process::id());
        let transport = TcpTransport::with_directory(
            ephemeral_addr,
            persist_dir,
            Arc::clone(&directory),
        );
        let manager = ShardManager::new(tapirs::Rng::from_seed(thread_rng().r#gen()), transport, Arc::clone(&directory));
        Self {
            manager: tokio::sync::Mutex::new(manager),
            directory,
            discovery_client: Arc::new(HttpDiscoveryClient::new(discovery_url)),
        }
    }
}

#[derive(Deserialize)]
struct JoinRequest {
    shard: u32,
    listen_addr: String,
}

async fn handle_request(
    state: &ShardManagerState,
    method: &str,
    path: &str,
    body: &str,
) -> (u16, String) {
    if method == "POST" && path == "/v1/join" {
        let req: JoinRequest = match serde_json::from_str(body) {
            Ok(r) => r,
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid JSON: {e}"}}"#));
            }
        };
        let shard = ShardNumber(req.shard);
        let new_addr: TcpAddress = match req.listen_addr.parse() {
            Ok(a) => TcpAddress(a),
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid listen_addr: {e}"}}"#));
            }
        };

        // Query discovery for existing membership.
        let existing = state
            .discovery_client
            .get_topology()
            .await
            .ok()
            .and_then(|t| t.shards.into_iter().find(|s| s.id == req.shard))
            .filter(|s| !s.replicas.is_empty());

        if let Some(entry) = existing {
            let addrs: Vec<TcpAddress> = match entry
                .replicas
                .iter()
                .map(|a| a.parse().map(TcpAddress))
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(a) => a,
                Err(e) => {
                    return (
                        500,
                        format!(r#"{{"error":"bad replica addr from discovery: {e}"}}"#),
                    );
                }
            };

            // Populate address directory so manager.join() can discover membership.
            state
                .directory
                .put(shard, IrMembership::new(addrs));

            match state.manager.lock().await.join(shard, new_addr).await {
                Ok(()) => (200, r#"{"ok":true}"#.to_string()),
                Err(e) => {
                    (
                        500,
                        format!(r#"{{"error":"join failed: {e}"}}"#),
                    )
                }
            }
        } else {
            // First replica — bootstrap via BootstrapRecord → StartView.
            state.manager.lock().await.bootstrap(shard, new_addr);
            (200, r#"{"ok":true}"#.to_string())
        }
    } else if method == "POST" && path == "/v1/leave" {
        let req: JoinRequest = match serde_json::from_str(body) {
            Ok(r) => r,
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid JSON: {e}"}}"#));
            }
        };
        let shard = ShardNumber(req.shard);
        let addr: TcpAddress = match req.listen_addr.parse() {
            Ok(a) => TcpAddress(a),
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid listen_addr: {e}"}}"#));
            }
        };

        // Query discovery for existing membership.
        let existing = state
            .discovery_client
            .get_topology()
            .await
            .ok()
            .and_then(|t| t.shards.into_iter().find(|s| s.id == req.shard))
            .filter(|s| !s.replicas.is_empty());

        let Some(entry) = existing else {
            return (
                400,
                format!(r#"{{"error":"shard {} not found in discovery"}}"#, req.shard),
            );
        };

        let addrs: Vec<TcpAddress> = match entry
            .replicas
            .iter()
            .map(|a| a.parse().map(TcpAddress))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(a) => a,
            Err(e) => {
                return (
                    500,
                    format!(r#"{{"error":"bad replica addr from discovery: {e}"}}"#),
                );
            }
        };

        // Verify the address is part of the shard.
        if !addrs.contains(&addr) {
            return (
                400,
                format!(
                    r#"{{"error":"address {} not in shard {}"}}"#,
                    req.listen_addr, req.shard
                ),
            );
        }

        // Populate address directory so manager.leave() can discover membership.
        state
            .directory
            .put(shard, IrMembership::new(addrs));

        match state.manager.lock().await.leave(shard, addr) {
            Ok(()) => (200, r#"{"ok":true}"#.to_string()),
            Err(e) => (500, format!(r#"{{"error":"leave failed: {e}"}}"#)),
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
        500 => "Internal Server Error",
        _ => "Unknown",
    }
}

pub(crate) async fn serve(listener: TcpListener, discovery_url: String) {
    let state = Arc::new(ShardManagerState::new(&discovery_url));

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let state = Arc::clone(&state);
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
                        if let Some(val) = trimmed.strip_prefix("Content-Length:") {
                            if let Ok(len) = val.trim().parse::<usize>() {
                                content_length = len;
                            }
                        }
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

                    let (status, response_body) =
                        handle_request(&state, method, path, &body).await;

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
                tracing::warn!("shard-manager accept error: {e}");
            }
        }
    }
}

pub async fn run(listen_addr: String, discovery_url: String) {
    let addr: std::net::SocketAddr = listen_addr
        .parse()
        .unwrap_or_else(|e| panic!("invalid shard-manager listen address '{listen_addr}': {e}"));

    let listener = TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("shard-manager: failed to bind {addr}: {e}"));

    tracing::info!(%addr, "shard-manager server starting");

    serve(listener, discovery_url).await;
}
