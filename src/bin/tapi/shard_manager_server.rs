use crate::discovery_backend::DiscoveryBackend;
use rand::{thread_rng, Rng as _};
use serde::Deserialize;
use std::sync::Arc;
use tapirs::discovery::{
    InMemoryShardDirectory, RemoteShardDirectory as _, ShardDirectory as _,
    strings_to_membership,
};
use tapirs::{IrMembership, KeyRange, ShardManager, ShardNumber, TapirReplica, TcpAddress, TcpTransport};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

type TapirShardManager = ShardManager<
    String,
    String,
    TcpTransport<TapirReplica<String, String>>,
    DiscoveryBackend,
>;

struct ShardManagerState {
    manager: tokio::sync::Mutex<TapirShardManager>,
    directory: Arc<InMemoryShardDirectory<TcpAddress>>,
    remote: Arc<DiscoveryBackend>,
}

impl ShardManagerState {
    fn new(
        remote: Arc<DiscoveryBackend>,
        #[cfg(feature = "tls")] tls_config: &Option<tapirs::tls::TlsConfig>,
    ) -> Self {
        let ephemeral_addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
            let a = l.local_addr().unwrap();
            drop(l);
            TcpAddress(a)
        };
        let directory = Arc::new(InMemoryShardDirectory::new());
        let persist_dir = format!("/tmp/tapi_shard_manager_{}", std::process::id());

        #[cfg(feature = "tls")]
        let transport = if let Some(tls) = tls_config {
            TcpTransport::with_tls(ephemeral_addr, persist_dir, Arc::clone(&directory), tls)
                .unwrap_or_else(|e| panic!("shard-manager transport TLS error: {e}"))
        } else {
            TcpTransport::with_directory(ephemeral_addr, persist_dir, Arc::clone(&directory))
        };
        #[cfg(not(feature = "tls"))]
        let transport = TcpTransport::with_directory(
            ephemeral_addr,
            persist_dir,
            Arc::clone(&directory),
        );

        let manager = ShardManager::new(
            tapirs::Rng::from_seed(thread_rng().r#gen()),
            transport,
            Arc::clone(&remote),
        );
        Self {
            manager: tokio::sync::Mutex::new(manager),
            directory,
            remote,
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
    if method == "GET" && path == "/healthz" {
        tracing::debug!("healthz: checking discovery connectivity");
        match tokio::time::timeout(std::time::Duration::from_secs(3), state.remote.weak_all_active_shard_view_memberships()).await {
            Ok(Ok(_)) => {
                return (200, r#"{"ok":true}"#.to_string());
            }
            Ok(Err(e)) => {
                return (
                    503,
                    format!(r#"{{"ok":false,"error":"discovery error: {e}"}}"#),
                );
            }
            Err(_) => {
                return (
                    503,
                    r#"{"ok":false,"error":"discovery timeout"}"#.to_string(),
                );
            }
        }
    }

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
            .remote
            .weak_all_active_shard_view_memberships()
            .await
            .ok()
            .and_then(|entries| entries.into_iter().find(|(s, _, _)| *s == shard))
            .map(|(_, m, v)| (m, v))
            .filter(|(m, _)| m.len() > 0);

        if let Some((membership, view)) = existing {
            // Populate address directory so manager.join() can discover membership.
            state.directory.put(shard, membership, view);

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
            // Shard not found in discovery — refuse to auto-bootstrap.
            // Initial shard creation must use static membership (--membership
            // flag in add-replica). The /v1/join path is only for adding a
            // replica to an existing shard already visible in discovery.
            (
                400,
                format!(r#"{{"error":"shard {} not found in discovery"}}"#, req.shard),
            )
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
        eprintln!("[leave-handler] querying discovery for shard={shard:?} to remove addr={addr:?}");
        let existing = state
            .remote
            .weak_all_active_shard_view_memberships()
            .await
            .ok()
            .and_then(|entries| entries.into_iter().find(|(s, _, _)| *s == shard))
            .map(|(_, m, v)| (m, v))
            .filter(|(m, _)| m.len() > 0);

        let Some((membership, view)) = existing else {
            eprintln!("[leave-handler] shard {shard:?} not found in discovery");
            return (
                400,
                format!(r#"{{"error":"shard {} not found in discovery"}}"#, req.shard),
            );
        };
        eprintln!("[leave-handler] discovery returned membership len={} view={view} for shard={shard:?}", membership.len());

        // Verify the address is part of the shard.
        if !membership.contains(addr) {
            eprintln!("[leave-handler] addr={addr:?} not in membership for shard={shard:?}");
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
            .put(shard, membership, view);

        eprintln!("[leave-handler] calling manager.leave(shard={shard:?}, addr={addr:?})");
        match state.manager.lock().await.leave(shard, addr).await {
            Ok(()) => (200, r#"{"ok":true}"#.to_string()),
            Err(e) => (500, format!(r#"{{"error":"leave failed: {e}"}}"#)),
        }
    } else if method == "POST" && path == "/v1/register" {
        #[derive(Deserialize)]
        struct RegisterRequest {
            shard: u32,
            key_range_start: Option<String>,
            key_range_end: Option<String>,
            #[serde(default)]
            replicas: Option<Vec<String>>,
        }
        let req: RegisterRequest = match serde_json::from_str(body) {
            Ok(r) => r,
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid JSON: {e}"}}"#));
            }
        };
        let shard = ShardNumber(req.shard);
        tracing::info!(?shard, "register: received request");

        // Use provided replicas or query discovery for shard membership.
        let (membership, view) = if let Some(ref replicas) = req.replicas {
            tracing::info!(?shard, replicas_count = replicas.len(), "register: using provided replicas");
            match strings_to_membership::<TcpAddress>(replicas) {
                Ok(m) => (m, 0u64),
                Err(e) => {
                    return (400, format!(r#"{{"error":"invalid replicas: {e}"}}"#));
                }
            }
        } else {
            tracing::info!(?shard, "register: querying discovery for membership");
            let existing = state
                .remote
                .weak_all_active_shard_view_memberships()
                .await
                .ok()
                .and_then(|entries| entries.into_iter().find(|(s, _, _)| *s == shard))
                .map(|(_, m, v)| (m, v))
                .filter(|(m, _)| m.len() > 0);

            match existing {
                Some((m, v)) => (m, v),
                None => {
                    return (
                        400,
                        format!(
                            r#"{{"error":"shard {} not found in discovery (provide replicas in request or populate discovery first)"}}"#,
                            req.shard
                        ),
                    );
                }
            }
        };

        state.directory.put(shard, membership.clone(), view);

        let key_range = KeyRange {
            start: req.key_range_start,
            end: req.key_range_end,
        };
        tracing::info!(?shard, ?key_range, "register: acquiring manager lock");
        let manager = state.manager.lock().await;
        tracing::info!(?shard, "register: calling register_active_shard (strong_atomic_update_shards)");
        manager.register_active_shard(shard, membership, key_range).await;
        drop(manager);
        tracing::info!(?shard, "register: completed successfully");
        (200, r#"{"ok":true}"#.to_string())
    } else if method == "POST" && path == "/v1/split" {
        #[derive(Deserialize)]
        struct SplitRequest {
            source: u32,
            split_key: String,
            new_shard: u32,
            new_replicas: Vec<String>,
        }
        let req: SplitRequest = match serde_json::from_str(body) {
            Ok(r) => r,
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid JSON: {e}"}}"#));
            }
        };

        let new_membership: IrMembership<TcpAddress> =
            match strings_to_membership(&req.new_replicas) {
                Ok(m) => m,
                Err(e) => {
                    return (400, format!(r#"{{"error":"invalid new_replicas: {e}"}}"#));
                }
            };

        state
            .directory
            .put(ShardNumber(req.new_shard), new_membership.clone(), 0);

        match state
            .manager
            .lock()
            .await
            .split(
                ShardNumber(req.source),
                req.split_key,
                ShardNumber(req.new_shard),
                new_membership,
            )
            .await
        {
            Ok(()) => (200, r#"{"ok":true}"#.to_string()),
            Err(e) => (500, format!(r#"{{"error":"split failed: {e:?}"}}"#)),
        }
    } else if method == "POST" && path == "/v1/merge" {
        #[derive(Deserialize)]
        struct MergeRequest {
            absorbed: u32,
            surviving: u32,
        }
        let req: MergeRequest = match serde_json::from_str(body) {
            Ok(r) => r,
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid JSON: {e}"}}"#));
            }
        };

        match state
            .manager
            .lock()
            .await
            .merge(ShardNumber(req.absorbed), ShardNumber(req.surviving))
            .await
        {
            Ok(()) => (200, r#"{"ok":true}"#.to_string()),
            Err(e) => (500, format!(r#"{{"error":"merge failed: {e:?}"}}"#)),
        }
    } else if method == "POST" && path == "/v1/compact" {
        #[derive(Deserialize)]
        struct CompactRequest {
            source: u32,
            new_shard: u32,
            new_replicas: Vec<String>,
        }
        let req: CompactRequest = match serde_json::from_str(body) {
            Ok(r) => r,
            Err(e) => {
                return (400, format!(r#"{{"error":"invalid JSON: {e}"}}"#));
            }
        };

        let new_membership: IrMembership<TcpAddress> =
            match strings_to_membership(&req.new_replicas) {
                Ok(m) => m,
                Err(e) => {
                    return (400, format!(r#"{{"error":"invalid new_replicas: {e}"}}"#));
                }
            };

        state
            .directory
            .put(ShardNumber(req.new_shard), new_membership.clone(), 0);

        match state
            .manager
            .lock()
            .await
            .compact(
                ShardNumber(req.source),
                ShardNumber(req.new_shard),
                new_membership,
            )
            .await
        {
            Ok(()) => (200, r#"{"ok":true}"#.to_string()),
            Err(e) => (500, format!(r#"{{"error":"compact failed: {e:?}"}}"#)),
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
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Unknown",
    }
}

pub(crate) async fn serve(
    listener: TcpListener,
    remote: Arc<DiscoveryBackend>,
    #[cfg(feature = "tls")] tls_acceptor: Option<tapirs::tls::ReloadableTlsAcceptor>,
    #[cfg(feature = "tls")] tls_config: Option<tapirs::tls::TlsConfig>,
) {
    let state = Arc::new(ShardManagerState::new(
        remote,
        #[cfg(feature = "tls")]
        &tls_config,
    ));

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let state = Arc::clone(&state);

                #[cfg(feature = "tls")]
                let tls_acceptor = tls_acceptor.clone();

                tokio::spawn(async move {
                    #[cfg(feature = "tls")]
                    if let Some(ref acceptor) = tls_acceptor {
                        let tls_acceptor = acceptor.acceptor();
                        match tls_acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                let (reader, writer) = tokio::io::split(tls_stream);
                                handle_connection(reader, writer, &state).await;
                            }
                            Err(e) => {
                                tracing::warn!("shard-manager TLS accept error: {e}");
                            }
                        }
                        return;
                    }

                    let (reader, writer) = stream.into_split();
                    handle_connection(reader, writer, &state).await;
                });
            }
            Err(e) => {
                tracing::warn!("shard-manager accept error: {e}");
            }
        }
    }
}

async fn handle_connection<R, W>(reader: R, mut writer: W, state: &ShardManagerState)
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
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

pub async fn run(
    listen_addr: String,
    discovery_tapir_endpoint: Option<String>,
    #[cfg(feature = "tls")] tls_config: Option<tapirs::tls::TlsConfig>,
) {
    let backend = if let Some(endpoint) = discovery_tapir_endpoint {
        // Create a separate transport for the discovery cluster.
        let ephemeral_addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
            let a = l.local_addr().unwrap();
            drop(l);
            TcpAddress(a)
        };
        let disc_dir = Arc::new(tapirs::discovery::InMemoryShardDirectory::new());
        let persist_dir = format!("/tmp/tapi_sm_disc_{}", std::process::id());
        #[cfg(feature = "tls")]
        let disc_transport = if let Some(ref tls) = tls_config {
            TcpTransport::with_tls(ephemeral_addr, persist_dir, disc_dir, tls)
                .unwrap_or_else(|e| panic!("discovery transport TLS error: {e}"))
        } else {
            TcpTransport::with_directory(ephemeral_addr, persist_dir, disc_dir)
        };
        #[cfg(not(feature = "tls"))]
        let disc_transport = TcpTransport::with_directory(ephemeral_addr, persist_dir, disc_dir);

        // Consistency: The shard-manager is the authority for shard membership
        // and route changes. It uses ReadMode::Strong (linearizable reads via
        // read-only TAPIR transactions with quorum validation) to ensure it
        // always reads the latest committed state. This is critical for
        // correctness: split/merge/compact decisions must be based on the
        // current shard layout, not stale data.
        let rng = tapirs::Rng::from_seed(thread_rng().r#gen());
        let dir = tapirs::discovery::tapir::parse_tapir_endpoint::<TcpAddress, _>(
            &endpoint,
            tapirs::discovery::tapir::ReadMode::Strong,
            disc_transport,
            rng,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: failed to create TAPIR discovery backend: {e}");
            std::process::exit(1);
        });
        DiscoveryBackend::Tapir(dir)
    } else {
        eprintln!("error: --discovery-tapir-endpoint is required");
        std::process::exit(1);
    };

    let addr: std::net::SocketAddr = listen_addr
        .parse()
        .unwrap_or_else(|e| panic!("invalid shard-manager listen address '{listen_addr}': {e}"));

    let listener = TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("shard-manager: failed to bind {addr}: {e}"));

    tracing::info!(%addr, "shard-manager server starting");

    #[cfg(feature = "tls")]
    let tls_acceptor = tls_config.as_ref().map(|c| {
        tapirs::tls::ReloadableTlsAcceptor::new(c)
            .unwrap_or_else(|e| panic!("shard-manager TLS config error: {e}"))
    });

    serve(
        listener,
        Arc::new(backend),
        #[cfg(feature = "tls")]
        tls_acceptor,
        #[cfg(feature = "tls")]
        tls_config,
    )
    .await;
}
