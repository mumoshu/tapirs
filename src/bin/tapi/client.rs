use crate::config::ClientConfig;
use std::sync::Arc;
use tapirs::{
    DynamicRouter, IrMembership, KeyRange, ShardDirectory, ShardEntry, ShardNumber,
    TapirClient, TcpAddress, TcpTransport, TapirReplica,
};
use std::sync::RwLock;

pub async fn run(cfg: ClientConfig) {
    if cfg.shards.is_empty() {
        eprintln!("error: no shards configured. Use --config or provide shard info.");
        std::process::exit(1);
    }

    // Ephemeral address for the client (random high port on loopback).
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let local_addr = listener.local_addr().unwrap();
    drop(listener);

    let address = TcpAddress(local_addr);
    let persist_dir = format!("/tmp/tapi_client_{}", std::process::id());
    let transport: TcpTransport<TapirReplica<String, String>> =
        TcpTransport::new(address, persist_dir);

    // Build shard directory entries and populate transport's shard addresses.
    let mut entries = Vec::new();
    for shard_cfg in &cfg.shards {
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
