pub(crate) mod admin_client;
pub(crate) mod admin_server;

pub use tapirs::node::{Node, ShardBackup};

use crate::config::NodeConfig;
use crate::discovery_backend::DiscoveryBackend;
use rand::{thread_rng, Rng as _};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tapirs::{ShardNumber, TcpAddress, TcpTransport};

pub(crate) fn production_rng() -> tapirs::Rng {
    tapirs::Rng::from_seed(thread_rng().r#gen())
}

pub async fn run(
    cfg: NodeConfig,
    discovery_json: Option<String>,
    discovery_tapir_endpoint: Option<String>,
    #[cfg(feature = "tls")] tls_config: Option<tapirs::tls::TlsConfig>,
) {
    let persist_dir = cfg
        .persist_dir
        .unwrap_or_else(|| "/tmp/tapi".to_string());
    let admin_listen_addr = cfg
        .admin_listen_addr
        .unwrap_or_else(|| "127.0.0.1:9000".to_string());

    let node = if let Some(json_path) = discovery_json {
        let backend = load_json_discovery_backend(&json_path).await;
        let mut node = Node::with_discovery_backend(persist_dir, backend, production_rng);
        if let Some(ref url) = cfg.shard_manager_url {
            node.shard_manager_url = Some(url.clone());
        }
        #[cfg(feature = "tls")]
        {
            node.tls_config = tls_config.clone();
        }
        Arc::new(node)
    } else if let Some(endpoint) = discovery_tapir_endpoint {
        let backend = load_tapir_discovery_backend(
            &endpoint,
            #[cfg(feature = "tls")]
            &tls_config,
        ).await;
        let mut node = Node::with_discovery_backend(persist_dir, backend, production_rng);
        if let Some(ref url) = cfg.shard_manager_url {
            node.shard_manager_url = Some(url.clone());
        }
        #[cfg(feature = "tls")]
        {
            node.tls_config = tls_config.clone();
        }
        Arc::new(node)
    } else {
        #[allow(unused_mut)] // mutated only when tls feature is enabled
        let mut node = Node::new(persist_dir, production_rng);
        #[cfg(feature = "tls")]
        {
            node.tls_config = tls_config.clone();
        }
        Arc::new(node)
    };

    for replica_cfg in &cfg.replicas {
        node.add_replica_no_join(replica_cfg).await.unwrap();
    }

    let admin_addr: SocketAddr = admin_listen_addr
        .parse()
        .unwrap_or_else(|e| panic!("invalid admin_listen_addr '{admin_listen_addr}': {e}"));

    #[cfg(feature = "tls")]
    let tls_acceptor = tls_config.as_ref().map(|c| {
        tapirs::tls::ReloadableTlsAcceptor::new(c)
            .unwrap_or_else(|e| panic!("admin server TLS config error: {e}"))
    });

    admin_server::start(
        admin_addr,
        Arc::clone(&node),
        #[cfg(feature = "tls")]
        tls_acceptor,
    )
    .await;

    if let Some(metrics_addr_str) = cfg.metrics_listen_addr {
        let metrics_addr: SocketAddr = metrics_addr_str
            .parse()
            .unwrap_or_else(|e| panic!("invalid metrics_listen_addr '{metrics_addr_str}': {e}"));
        crate::metrics_server::start(metrics_addr, Arc::clone(&node)).await;
    }

    tracing::info!(%admin_listen_addr, "node ready, press Ctrl-C to stop");

    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for Ctrl-C");
    tracing::info!("shutting down");
}

/// Parse a `--discovery-json` file and build a `DiscoveryBackend::Json`.
///
/// Same JSON format as `tapi client --discovery-json`:
/// - Static: `{"shards":[{"number":0,"membership":["addr:port",...]}]}`
/// - DNS: `{"shards":[{"number":0,"headless_service":"svc.ns:port"}]}`
async fn load_json_discovery_backend(json_path: &str) -> DiscoveryBackend {
    use tapirs::discovery::json::JsonRemoteShardDirectory;

    #[derive(serde::Deserialize)]
    struct DiscoveryJson {
        shards: Vec<DiscoveryJsonShard>,
    }
    #[derive(serde::Deserialize)]
    struct DiscoveryJsonShard {
        number: u32,
        #[serde(default)]
        membership: Vec<String>,
        #[serde(default)]
        headless_service: Option<String>,
    }

    let content = std::fs::read_to_string(json_path)
        .unwrap_or_else(|e| panic!("failed to read discovery JSON {json_path}: {e}"));
    let discovery: DiscoveryJson = serde_json::from_str(&content)
        .unwrap_or_else(|e| panic!("failed to parse discovery JSON {json_path}: {e}"));

    // Separate static and DNS shards.
    let mut static_shards = Vec::new();
    let mut dns_shards = Vec::new();

    for shard in discovery.shards {
        let shard_num = ShardNumber(shard.number);
        if let Some(ref headless) = shard.headless_service {
            let (host, port_str) = headless.rsplit_once(':')
                .unwrap_or_else(|| panic!("headless_service '{headless}' missing :port"));
            let port: u16 = port_str.parse()
                .unwrap_or_else(|e| panic!("invalid port in '{headless}': {e}"));
            dns_shards.push((shard_num, host.to_string(), port));
        } else {
            let membership = tapirs::discovery::strings_to_membership::<TcpAddress>(&shard.membership)
                .unwrap_or_else(|e| panic!("invalid membership for shard {}: {e}", shard.number));
            static_shards.push((shard_num, membership));
        }
    }

    let dir = if dns_shards.is_empty() {
        JsonRemoteShardDirectory::new(static_shards)
    } else if static_shards.is_empty() {
        JsonRemoteShardDirectory::with_dns(dns_shards, Duration::from_secs(30))
            .await
            .unwrap_or_else(|e| panic!("DNS discovery failed: {e}"))
    } else {
        // Mixed mode: build DNS directory first, then add static entries.
        // Not expected in practice but handle gracefully.
        let mut dir = JsonRemoteShardDirectory::with_dns(dns_shards, Duration::from_secs(30))
            .await
            .unwrap_or_else(|e| panic!("DNS discovery failed: {e}"));
        dir.add_static_shards(static_shards);
        dir
    };

    DiscoveryBackend::Json(dir)
}

/// Parse a `--discovery-tapir-endpoint` and build a `DiscoveryBackend::Tapir`.
///
/// Uses eventual consistent reads (unlogged scan) — suitable for node
/// shard discovery via CachingShardDirectory PULL.
async fn load_tapir_discovery_backend(
    endpoint: &str,
    #[cfg(feature = "tls")] tls_config: &Option<tapirs::tls::TlsConfig>,
) -> DiscoveryBackend {
    use tapirs::discovery::tapir;

    let ephemeral_addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let a = l.local_addr().unwrap();
        drop(l);
        TcpAddress(a)
    };
    let disc_dir = Arc::new(tapirs::discovery::InMemoryShardDirectory::new());
    let persist_dir = format!("/tmp/tapi_node_disc_{}", std::process::id());

    #[cfg(feature = "tls")]
    let disc_transport = if let Some(tls) = tls_config {
        TcpTransport::with_tls(ephemeral_addr, persist_dir, disc_dir, tls)
            .unwrap_or_else(|e| panic!("discovery transport TLS error: {e}"))
    } else {
        TcpTransport::with_directory(ephemeral_addr, persist_dir, disc_dir)
    };
    #[cfg(not(feature = "tls"))]
    let disc_transport = TcpTransport::with_directory(ephemeral_addr, persist_dir, disc_dir);

    // Consistency: Nodes use eventual reads (weak_* methods: unlogged scan
    // to 1 random replica) for shard discovery. Nodes tolerate stale reads because
    // CachingShardDirectory syncs periodically and the node retries on
    // OutOfRange. The shard-manager (not the node) is the authority for
    // consistent membership state.
    let rng = production_rng();
    let dir = tapir::parse_tapir_endpoint::<TcpAddress, _>(
        endpoint,
        disc_transport,
        rng,
    )
    .await
    .unwrap_or_else(|e| {
        eprintln!("error: failed to create TAPIR discovery backend: {e}");
        std::process::exit(1);
    });
    DiscoveryBackend::Tapir(dir)
}
