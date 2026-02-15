use crate::config::{ClientConfig, ShardConfig};
use crate::discovery::{ClusterTopology, ShardMembership};
use std::sync::Arc;
use tapirs::{
    DynamicRouter, IrMembership, KeyRange, ShardDirectory, ShardEntry, ShardNumber,
    TapirClient, TcpAddress, TcpTransport, TapirReplica,
};
use std::sync::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn fetch_shards_from_discovery(discovery_url: &str) -> Vec<ShardConfig> {
    let addr_str = discovery_url
        .strip_prefix("http://")
        .unwrap_or(discovery_url);
    let addr: std::net::SocketAddr = addr_str
        .parse()
        .unwrap_or_else(|e| panic!("invalid discovery_url '{discovery_url}': {e}"));

    let request = format!(
        "GET /v1/cluster HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        addr_str,
    );

    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to connect to discovery service at {addr}: {e}"));
    stream
        .write_all(request.as_bytes())
        .await
        .unwrap_or_else(|e| panic!("failed to write to discovery service: {e}"));
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .unwrap_or_else(|e| panic!("failed to read from discovery service: {e}"));

    let resp_str = String::from_utf8_lossy(&response);
    // Find the JSON body after the blank line separating headers from body.
    let body = resp_str
        .split_once("\r\n\r\n")
        .map(|(_, b)| b)
        .unwrap_or(&resp_str);

    let topology: ClusterTopology = serde_json::from_str(body)
        .unwrap_or_else(|e| panic!("invalid response from discovery service: {e}\nbody: {body}"));

    topology
        .shards
        .into_iter()
        .map(|ShardMembership { id, replicas }| ShardConfig { id, replicas })
        .collect()
}

pub async fn run(cfg: ClientConfig) {
    let shards = if cfg.shards.is_empty() {
        if let Some(ref url) = cfg.discovery_url {
            fetch_shards_from_discovery(url).await
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
    let transport: TcpTransport<TapirReplica<String, String>> =
        TcpTransport::new(address, persist_dir);

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
