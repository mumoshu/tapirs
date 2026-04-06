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
    #[cfg(feature = "tls")] tls_config: Option<tapirs::tls::TlsConfig>,
) -> i32 {
    let shards = if let Some(json_path) = discovery_json {
        load_discovery_json(&json_path).await
    } else if let Some(ref endpoint) = discovery_tapir_endpoint {
        load_tapir_discovery(
            endpoint,
            #[cfg(feature = "tls")]
            tls_config.as_ref(),
        ).await
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
    let address_directory = Arc::new(InMemoryShardDirectory::new());

    #[cfg(feature = "tls")]
    let transport: TcpTransport<TapirReplica<String, String>> = if let Some(ref tls_cfg) = tls_config {
        TcpTransport::with_tls(address, Arc::clone(&address_directory), tls_cfg)
            .unwrap_or_else(|e| panic!("client TLS config error: {e}"))
    } else {
        TcpTransport::with_directory(address, Arc::clone(&address_directory))
    };

    #[cfg(not(feature = "tls"))]
    let transport: TcpTransport<TapirReplica<String, String>> =
        TcpTransport::with_directory(address, Arc::clone(&address_directory));

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

    // Build key range entries from discovery or static config. A single shard
    // with no explicit ranges covers the entire key space [None, None). Multiple
    // shards MUST have explicit key ranges — the client never fabricates ranges.
    if shards.len() > 1 {
        let missing: Vec<_> = shards
            .iter()
            .filter(|s| s.key_range_start.is_none() && s.key_range_end.is_none())
            .map(|s| s.id)
            .collect();
        if !missing.is_empty() {
            eprintln!(
                "error: {} shard(s) have no key range (shard IDs: {:?}). \
                 With multiple shards, every shard must have explicit key ranges \
                 from discovery or static configuration.",
                missing.len(),
                missing,
            );
            std::process::exit(1);
        }
    }
    let entries: Vec<ShardEntry<String>> = shards
        .iter()
        .map(|s| ShardEntry {
            shard: ShardNumber(s.id),
            range: KeyRange {
                start: s.key_range_start.clone(),
                end: s.key_range_end.clone(),
            },
        })
        .collect();

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
const DISCOVERY_INITIAL_BACKOFF: std::time::Duration = std::time::Duration::from_millis(200);
const DISCOVERY_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);
const DISCOVERY_MAX_RETRIES: u32 = 10;

/// Fetch shard topology from a TAPIR discovery cluster endpoint.
///
/// # Consistency model
///
/// The shard-manager writes shard membership and route changes to the TAPIR
/// discovery cluster via **strongly-consistent** transactions (quorum writes).
/// The client reads them via **eventually-consistent** reads (`weak_*` methods
/// = unlogged scan to 1 random replica) for lower latency and load.
///
/// Because of this asymmetry, the client may read stale data right after the
/// shard-manager writes. Both `all()` (shard membership) and
/// `route_changes_since()` (key range changelog) are retried with exponential
/// backoff until the reads are complete. The client never fabricates key
/// ranges — it uses discovery-provided ranges or fails.
async fn load_tapir_discovery(
    endpoint: &str,
    #[cfg(feature = "tls")] tls_config: Option<&tapirs::tls::TlsConfig>,
) -> Vec<ShardConfig> {
    use std::collections::BTreeMap;
    use tapirs::discovery::{tapir, RemoteShardDirectory, ShardDirectoryChange};

    let ephemeral_addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let a = l.local_addr().unwrap();
        drop(l);
        TcpAddress(a)
    };
    let disc_dir = Arc::new(tapirs::discovery::InMemoryShardDirectory::new());
    #[cfg(feature = "tls")]
    let disc_transport: TcpTransport<TapirReplica<String, String>> = if let Some(tls_cfg) = tls_config {
        TcpTransport::with_tls(ephemeral_addr, disc_dir, tls_cfg)
            .unwrap_or_else(|e| panic!("discovery TLS config error: {e}"))
    } else {
        TcpTransport::with_directory(ephemeral_addr, disc_dir)
    };

    #[cfg(not(feature = "tls"))]
    let disc_transport: TcpTransport<TapirReplica<String, String>> =
        TcpTransport::with_directory(ephemeral_addr, disc_dir);

    let rng = tapirs::Rng::from_seed(thread_rng().r#gen());
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

    // Strong read (quorum scan) instead of eventual (unlogged to 1 replica).
    //
    // An eventual read would eventually return the correct membership, but
    // "eventually" depends on FinalizeInconsistent(Commit) propagating to
    // the specific replica that serves the unlogged scan. If that replica
    // missed the finalize (e.g. it was ViewChanging when the finalize was
    // sent), it won't have the committed shard entry until the next view
    // change merges it — which can take 10+ seconds.
    //
    // This matters for short-lived clients (REPL -e mode, kubectl run pods)
    // that execute queries immediately after connecting. A strong read
    // guarantees the client sees all committed shard entries on the first
    // attempt. The cost is one quorum scan at startup — negligible for a
    // one-time topology load.
    let entries = {
        let mut backoff = DISCOVERY_INITIAL_BACKOFF;
        let mut entries = Vec::new();
        for attempt in 0..=DISCOVERY_MAX_RETRIES {
            match <_ as RemoteShardDirectory<TcpAddress, String>>::strong_all_active_shard_view_memberships(&dir).await {
                Ok(e) => entries = e,
                Err(e) => {
                    // Retry on transient errors (discovery replicas may be
                    // ViewChanging during cluster bootstrap, causing the
                    // quorum scan to fail with Unavailable).
                    if attempt < DISCOVERY_MAX_RETRIES {
                        eprintln!(
                            "warning: discovery topology scan failed (attempt {}/{}): {e:?}, \
                             retrying in {backoff:?}...",
                            attempt + 1,
                            DISCOVERY_MAX_RETRIES,
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(DISCOVERY_MAX_BACKOFF);
                        continue;
                    }
                    panic!("failed to fetch topology from TAPIR discovery after {} retries: {e}", DISCOVERY_MAX_RETRIES);
                }
            }
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
    // assignments. strong_atomic_update_shards() writes atomic changesets during
    // register_active_shard/split/merge/compact — replaying from index 0 gives us
    // the latest key ranges.
    //
    // Consistency: weak_route_changes_since() uses eventual reads (unlogged
    // read to 1 random replica). The shard-manager writes route changes via
    // strongly-consistent TAPIR transactions, but the client reads them
    // eventually consistently — the read replica may not yet have the latest
    // changesets. With multiple shards, every shard MUST have an explicit key
    // range. If route changes haven't propagated yet, retry with backoff
    // until all shards have ranges.
    let shard_ids: Vec<ShardNumber> = entries.iter().map(|(s, _, _)| *s).collect();
    let need_ranges = entries.len() > 1;

    let key_ranges = {
        let mut backoff = DISCOVERY_INITIAL_BACKOFF;
        let mut key_ranges: BTreeMap<ShardNumber, KeyRange<String>> = BTreeMap::new();

        for attempt in 0..=DISCOVERY_MAX_RETRIES {
            let changesets =
                <_ as RemoteShardDirectory<TcpAddress, String>>::weak_route_changes_since(&dir, 0)
                    .await
                    .unwrap_or_else(|e| {
                        eprintln!(
                            "warning: failed to read route changelog from TAPIR discovery: {e}"
                        );
                        vec![]
                    });

            key_ranges.clear();
            for (_idx, changes) in changesets {
                for change in changes {
                    match change {
                        ShardDirectoryChange::ActivateShard { shard, range, .. } => {
                            key_ranges.insert(shard, range);
                        }
                        ShardDirectoryChange::TombstoneShard { shard } => {
                            key_ranges.remove(&shard);
                        }
                    }
                }
            }

            if !need_ranges {
                break;
            }

            // Check that every shard from all() has a key range.
            let missing: Vec<_> = shard_ids
                .iter()
                .filter(|s| !key_ranges.contains_key(s))
                .collect();
            if missing.is_empty() {
                break;
            }

            if attempt == DISCOVERY_MAX_RETRIES {
                panic!(
                    "TAPIR discovery route changelog missing key ranges for shard(s) {:?} \
                     after {} retries (endpoint: {endpoint}). The shard-manager writes \
                     route changes via strongly-consistent transactions, but client reads \
                     are eventually consistent — the discovery replicas may not have \
                     propagated yet. Is the discovery cluster healthy?",
                    missing, DISCOVERY_MAX_RETRIES,
                );
            }
            eprintln!(
                "warning: route changelog missing key ranges for shard(s) {:?} \
                 (attempt {}/{}, retrying in {:?}...)",
                missing,
                attempt + 1,
                DISCOVERY_MAX_RETRIES,
                backoff,
            );
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(DISCOVERY_MAX_BACKOFF);
        }
        key_ranges
    };

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

