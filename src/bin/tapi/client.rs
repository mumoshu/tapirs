use crate::config::{ClientConfig, ShardConfig};
use crate::discovery::HttpDiscoveryClient;
use std::sync::Arc;
use tapirs::discovery::{
    DiscoveryClient as _, DiscoveryShardDirectory, InMemoryShardDirectory,
};
use tapirs::{
    DynamicRouter, IrMembership, KeyRange, ShardDirectory, ShardEntry, ShardNumber,
    TapirClient, TcpAddress, TcpTransport, TapirReplica,
};
use std::sync::RwLock;

pub async fn run(cfg: ClientConfig) {
    let shards = if cfg.shards.is_empty() {
        if let Some(ref url) = cfg.discovery_url {
            let client = HttpDiscoveryClient::new(url);
            let topology = client
                .get_topology()
                .await
                .unwrap_or_else(|e| panic!("failed to fetch topology from discovery: {e}"));
            topology
                .shards
                .into_iter()
                .map(|s| ShardConfig {
                    id: s.id,
                    replicas: s.replicas,
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

    // If discovery is configured, create DiscoveryShardDirectory for continuous
    // updates (learns about new shards and membership changes from other nodes).
    let _discovery_dir = cfg.discovery_url.as_ref().map(|url| {
        let client = Arc::new(HttpDiscoveryClient::new(url));
        DiscoveryShardDirectory::<TcpAddress, _>::new(
            Arc::clone(&address_directory),
            client,
            std::time::Duration::from_secs(10),
        )
    });

    let transport: TcpTransport<TapirReplica<String, String>> =
        TcpTransport::with_directory(address, persist_dir, Arc::clone(&address_directory));

    // Build shard directory entries and populate transport's shard addresses.
    let mut entries = Vec::new();
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

        entries.push(ShardEntry {
            shard,
            range: KeyRange {
                start: None,
                end: None,
            },
        });
    }

    let directory = Arc::new(RwLock::new(ShardDirectory::new(entries)));
    let router = Arc::new(DynamicRouter::new(directory));
    let tapir_client = Arc::new(TapirClient::new(transport));

    crate::repl::run(tapir_client, router).await;
}
