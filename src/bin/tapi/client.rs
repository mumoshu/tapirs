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

pub async fn run(cfg: ClientConfig, input_source: crate::repl::InputSource) -> i32 {
    let shards = if cfg.shards.is_empty() {
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
            eprintln!("error: no shards configured. Use --config or provide shard info.");
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
