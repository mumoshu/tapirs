use crate::config::{ClientConfig, ShardConfig};
use crate::discovery::HttpDiscoveryClient;
use rand::{thread_rng, Rng as _};
use std::sync::Arc;
use tapirs::discovery::{
    CachingShardDirectory, InMemoryShardDirectory, RemoteShardDirectory as _,
};
use tapirs::{
    DynamicRouter, IrMembership, KeyRange, ShardDirectory, ShardEntry, ShardNumber,
    TapirClient, TcpAddress, TcpTransport, TapirReplica,
};
use std::sync::RwLock;

/// JSON schema for `--discovery-json` shard topology file.
#[derive(serde::Deserialize)]
struct DiscoveryJson {
    shards: Vec<DiscoveryJsonShard>,
}

#[derive(serde::Deserialize)]
struct DiscoveryJsonShard {
    number: u32,
    /// Explicit replica addresses (static mode).
    #[serde(default)]
    membership: Vec<String>,
    /// Headless service "host:port" resolved via DNS at startup (DNS mode).
    #[serde(default)]
    headless_service: Option<String>,
    #[serde(default)]
    key_range_start: Option<String>,
    #[serde(default)]
    key_range_end: Option<String>,
}

pub async fn run(
    cfg: ClientConfig,
    discovery_json: Option<String>,
    discovery_tapir_endpoint: Option<String>,
    input_source: crate::repl::InputSource,
) -> i32 {
    let shards = if let Some(json_path) = discovery_json {
        load_discovery_json(&json_path).await
    } else if let Some(ref endpoint) = discovery_tapir_endpoint {
        load_tapir_discovery(endpoint).await
    } else if cfg.shards.is_empty() {
        if let Some(ref url) = cfg.discovery_url {
            let client = HttpDiscoveryClient::new(url);
            let entries = client
                .all()
                .await
                .unwrap_or_else(|e| panic!("failed to fetch topology from discovery: {e}"));
            entries
                .into_iter()
                .map(|(shard, membership, _view)| ShardConfig {
                    id: shard.0,
                    replicas: tapirs::discovery::membership_to_strings(&membership),
                    key_range_start: None,
                    key_range_end: None,
                })
                .collect()
        } else {
            eprintln!("error: no shards configured. Use --config, --discovery-url, --discovery-json, or --discovery-tapir-endpoint.");
            std::process::exit(1);
        }
    } else {
        cfg.shards
    };

    // Ephemeral address for the client (random high port on loopback).
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let local_addr = listener.local_addr().unwrap();
    drop(listener);

    let address = TcpAddress(local_addr);
    let persist_dir = format!("/tmp/tapi_client_{}", std::process::id());

    let address_directory = Arc::new(InMemoryShardDirectory::new());

    // If discovery is configured, create CachingShardDirectory for continuous
    // updates (learns about new shards and membership changes from other nodes).
    let _discovery_dir = cfg.discovery_url.as_ref().map(|url| {
        let client = Arc::new(HttpDiscoveryClient::new(url));
        CachingShardDirectory::<TcpAddress, _>::new(
            Arc::clone(&address_directory),
            client,
            std::time::Duration::from_secs(10),
        )
    });

    let transport: TcpTransport<TapirReplica<String, String>> =
        TcpTransport::with_directory(address, persist_dir, Arc::clone(&address_directory));

    // Build shard directory entries and populate transport's shard addresses.
    // Populate membership first, then build key range entries.
    for shard_cfg in &shards {
        let shard = ShardNumber(shard_cfg.id);
        let addrs: Vec<TcpAddress> = shard_cfg
            .replicas
            .iter()
            .map(|a| {
                TcpAddress(
                    a.parse()
                        .unwrap_or_else(|e| panic!("invalid replica addr '{a}': {e}")),
                )
            })
            .collect();
        let membership = IrMembership::new(addrs);
        transport.set_shard_addresses(shard, membership);
    }

    // Build key range entries. If any shard has explicit key_range fields, use
    // them. Otherwise auto-partition the key space (a-z) evenly across shards.
    let has_explicit_ranges = shards
        .iter()
        .any(|s| s.key_range_start.is_some() || s.key_range_end.is_some());

    let entries: Vec<ShardEntry<String>> = if has_explicit_ranges || shards.len() <= 1 {
        shards
            .iter()
            .map(|s| ShardEntry {
                shard: ShardNumber(s.id),
                range: KeyRange {
                    start: s.key_range_start.clone(),
                    end: s.key_range_end.clone(),
                },
            })
            .collect()
    } else {
        build_shard_entries(shards.len() as u32)
    };

    let directory = Arc::new(RwLock::new(ShardDirectory::new(entries)));
    let router = Arc::new(DynamicRouter::new(directory));
    let tapir_client = Arc::new(TapirClient::new(tapirs::Rng::from_seed(thread_rng().r#gen()), transport));

    crate::repl::run(tapir_client, router, input_source).await
}

/// Parse a `--discovery-json` file into shard configs.
///
/// Static mode entries provide explicit `membership` addresses. DNS mode entries
/// specify a `headless_service` hostname that is resolved via DNS at startup.
async fn load_discovery_json(json_path: &str) -> Vec<ShardConfig> {
    let content = std::fs::read_to_string(json_path)
        .unwrap_or_else(|e| panic!("failed to read discovery JSON {json_path}: {e}"));
    let discovery: DiscoveryJson = serde_json::from_str(&content)
        .unwrap_or_else(|e| panic!("failed to parse discovery JSON {json_path}: {e}"));

    let mut shard_configs = Vec::new();
    for shard in discovery.shards {
        let replicas = if let Some(ref headless) = shard.headless_service {
            // DNS mode: resolve hostname at startup.
            let mut addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host(headless)
                .await
                .unwrap_or_else(|e| panic!("failed to resolve DNS for '{headless}': {e}"))
                .collect();
            if addrs.is_empty() {
                panic!("DNS resolution for '{headless}' returned zero addresses");
            }
            addrs.sort();
            addrs.iter().map(|a| a.to_string()).collect()
        } else {
            // Static mode: addresses provided directly.
            shard.membership
        };

        shard_configs.push(ShardConfig {
            id: shard.number,
            replicas,
            key_range_start: shard.key_range_start,
            key_range_end: shard.key_range_end,
        });
    }
    shard_configs
}

/// Fetch shard topology from a TAPIR discovery cluster endpoint.
///
/// Uses eventual consistent reads (unlogged scan to 1 random replica).
async fn load_tapir_discovery(endpoint: &str) -> Vec<ShardConfig> {
    use tapirs::discovery::{tapir, RemoteShardDirectory as _};

    let ephemeral_addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let a = l.local_addr().unwrap();
        drop(l);
        TcpAddress(a)
    };
    let disc_dir = Arc::new(tapirs::discovery::InMemoryShardDirectory::new());
    let persist_dir = format!("/tmp/tapi_client_disc_{}", std::process::id());
    let disc_transport: TcpTransport<TapirReplica<String, String>> =
        TcpTransport::with_directory(ephemeral_addr, persist_dir, disc_dir);

    let rng = tapirs::Rng::from_seed(thread_rng().r#gen());
    let dir = tapir::parse_tapir_endpoint::<TcpAddress, _>(
        endpoint,
        tapir::ReadMode::Eventual,
        disc_transport,
        rng,
    )
    .await
    .unwrap_or_else(|e| {
        eprintln!("error: failed to create TAPIR discovery backend: {e}");
        std::process::exit(1);
    });

    let entries = dir
        .all()
        .await
        .unwrap_or_else(|e| panic!("failed to fetch topology from TAPIR discovery: {e}"));

    entries
        .into_iter()
        .map(|(shard, membership, _view)| ShardConfig {
            id: shard.0,
            replicas: tapirs::discovery::membership_to_strings(&membership),
            key_range_start: None,
            key_range_end: None,
        })
        .collect()
}

/// Partition key space (a-z) evenly across N shards with sequential IDs 0..N.
fn build_shard_entries(n: u32) -> Vec<ShardEntry<String>> {
    if n == 0 {
        return vec![];
    }
    if n == 1 {
        return vec![ShardEntry {
            shard: ShardNumber(0),
            range: KeyRange {
                start: None,
                end: None,
            },
        }];
    }

    let chars: Vec<char> = ('a'..='z').collect();
    let per = chars.len() / n as usize;
    let mut entries = Vec::new();
    for i in 0..n {
        let start = if i == 0 {
            None
        } else {
            Some(chars[i as usize * per].to_string())
        };
        let end = if i == n - 1 {
            None
        } else {
            Some(chars[(i as usize + 1) * per].to_string())
        };
        entries.push(ShardEntry {
            shard: ShardNumber(i),
            range: KeyRange { start, end },
        });
    }
    entries
}
