use crate::config::{ClientConfig, ShardConfig};
use rand::{thread_rng, Rng as _};
use std::sync::Arc;
use tapirs::discovery::InMemoryShardDirectory;
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
        eprintln!("error: no shards configured. Use --config, --discovery-json, or --discovery-tapir-endpoint.");
        std::process::exit(1);
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

/// Backoff constants for waiting on eventually-consistent discovery reads.
const DISCOVERY_INITIAL_BACKOFF: std::time::Duration = std::time::Duration::from_millis(500);
const DISCOVERY_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(5);
const DISCOVERY_MAX_RETRIES: u32 = 10;

/// Fetch shard topology from a TAPIR discovery cluster endpoint.
///
/// Uses eventual consistent reads (unlogged scan to 1 random replica).
/// Retries with exponential backoff until a non-empty shard list is returned,
/// since both `all()` and `publish_route_changes()` are eventually consistent.
async fn load_tapir_discovery(endpoint: &str) -> Vec<ShardConfig> {
    use std::collections::HashMap;
    use tapirs::discovery::{tapir, RemoteShardDirectory, ShardDirectoryChange};

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

    // Both all() and route_changes_since() use ReadMode::Eventual (unlogged
    // scan to 1 random replica). This is eventually consistent: after cluster
    // bootstrap or shard-manager operations (register_shard, split, merge,
    // compact), a replica may not yet have the latest writes. Retry with
    // exponential backoff until we get a non-empty shard list.
    //
    // Similarly, publish_route_changes() writes route changelog entries via
    // eventually-consistent TAPIR transactions. The route changelog read
    // below may return stale or empty results if the discovery cluster
    // hasn't fully propagated the writes yet.
    let entries = {
        let mut backoff = DISCOVERY_INITIAL_BACKOFF;
        let mut entries = Vec::new();
        for attempt in 0..=DISCOVERY_MAX_RETRIES {
            entries = <_ as RemoteShardDirectory<TcpAddress, String>>::all(&dir)
                .await
                .unwrap_or_else(|e| {
                    panic!("failed to fetch topology from TAPIR discovery: {e}")
                });
            if !entries.is_empty() {
                break;
            }
            if attempt == DISCOVERY_MAX_RETRIES {
                panic!(
                    "TAPIR discovery returned 0 shards after {} retries \
                     (endpoint: {endpoint}). Is the discovery cluster healthy \
                     and populated?",
                    DISCOVERY_MAX_RETRIES
                );
            }
            eprintln!(
                "warning: TAPIR discovery returned 0 shards (attempt {}/{}), \
                 retrying in {:?}... (eventual consistency propagation delay)",
                attempt + 1,
                DISCOVERY_MAX_RETRIES,
                backoff,
            );
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(DISCOVERY_MAX_BACKOFF);
        }
        entries
    };

    // Replay the full route changelog to extract current key range
    // assignments. publish_route_changes() writes atomic changesets during
    // register_shard/split/merge/compact — replaying from index 0 gives us
    // the latest key ranges. This is also an eventually-consistent read;
    // if the changelog hasn't propagated yet we proceed without key ranges
    // (falling back to auto-partition or None).
    let changesets =
        <_ as RemoteShardDirectory<TcpAddress, String>>::route_changes_since(&dir, 0)
            .await
            .unwrap_or_else(|e| {
                eprintln!(
                    "warning: failed to read route changelog from TAPIR discovery: {e}. \
                     Proceeding without explicit key ranges."
                );
                vec![]
            });
    let mut key_ranges: HashMap<ShardNumber, KeyRange<String>> = HashMap::new();
    for (_idx, changes) in changesets {
        for change in changes {
            match change {
                ShardDirectoryChange::SetRange { shard, range, .. } => {
                    key_ranges.insert(shard, range);
                }
                ShardDirectoryChange::RemoveRange { shard } => {
                    key_ranges.remove(&shard);
                }
            }
        }
    }

    entries
        .into_iter()
        .map(|(shard, membership, _view)| {
            let range = key_ranges.get(&shard);
            ShardConfig {
                id: shard.0,
                replicas: tapirs::discovery::membership_to_strings(&membership),
                key_range_start: range.and_then(|r| r.start.clone()),
                key_range_end: range.and_then(|r| r.end.clone()),
            }
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
