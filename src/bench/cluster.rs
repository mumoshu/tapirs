#![allow(dead_code, unused_imports)]

use super::{BenchRoutingClient, BenchTapirClient, BenchTransport};
use crate::discovery::{InMemoryShardDirectory, ShardDirectory as _};
use crate::node::{Node, ReplicaConfig};
use crate::{
    DynamicRouter, IrMembership, KeyRange, ShardDirectory, ShardEntry, ShardNumber, TcpAddress,
    TcpTransport,
};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tempfile::TempDir;

pub struct BenchTarget {
    pub replica_addrs: Vec<SocketAddr>,
}

pub(crate) struct BenchCluster {
    pub nodes: Vec<Arc<Node>>,
    _temp_dirs: Vec<TempDir>,
}

/// Monotonic counter for unique RNG seeds (avoids thread_rng in library code).
static RNG_COUNTER: AtomicU64 = AtomicU64::new(1000);

fn bench_rng() -> crate::Rng {
    let seed = RNG_COUNTER.fetch_add(1, Ordering::Relaxed);
    crate::Rng::from_seed(seed)
}

fn alloc_listener() -> (SocketAddr, std::net::TcpListener) {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    (a, l)
}

fn alloc_addr() -> SocketAddr {
    alloc_listener().0
}

pub async fn bootstrap_cluster(
    config: &super::ClusterConfig,
) -> (BenchTarget, BenchCluster) {
    let n = config.num_replicas;
    let mut listeners = Vec::with_capacity(n);
    for _ in 0..n {
        listeners.push(alloc_listener());
    }
    let addrs: Vec<SocketAddr> = listeners.iter().map(|(a, _)| *a).collect();
    let membership: Vec<String> = addrs.iter().map(|a| a.to_string()).collect();

    let mut nodes = Vec::with_capacity(n);
    let mut temp_dirs = Vec::with_capacity(n);
    for (i, (addr, listener)) in listeners.into_iter().enumerate() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let persist_dir = temp_dir.path().to_str().unwrap().to_string();
        let node = Arc::new(Node::new(persist_dir, bench_rng));
        let cfg = ReplicaConfig {
            shard: 0,
            listen_addr: addr.to_string(),
            membership: membership.clone(),
        };
        node.add_replica_with_listener(&cfg, listener)
            .await
            .unwrap_or_else(|e| panic!("node {i} add_replica failed: {e}"));
        nodes.push(node);
        temp_dirs.push(temp_dir);
    }

    let target = BenchTarget { replica_addrs: addrs };
    let cluster = BenchCluster { nodes, _temp_dirs: temp_dirs };
    (target, cluster)
}

pub fn external_target(addrs_str: &str) -> BenchTarget {
    let addrs = addrs_str
        .split(',')
        .map(|s| s.trim().parse().expect("invalid address"))
        .collect();
    BenchTarget { replica_addrs: addrs }
}

impl BenchTarget {
    pub fn create_client(&self) -> Arc<BenchRoutingClient> {
        let directory = Arc::new(InMemoryShardDirectory::new());
        let tcp_addrs: Vec<TcpAddress> =
            self.replica_addrs.iter().map(|a| TcpAddress(*a)).collect();
        let membership = IrMembership::new(tcp_addrs);
        directory.put(ShardNumber(0), membership, 0);

        let client_addr = TcpAddress(alloc_addr());
        let transport: BenchTransport =
            TcpTransport::with_directory(client_addr, directory);

        let rng = bench_rng();
        let tapir_client = Arc::new(BenchTapirClient::new(rng, transport));

        let shard_dir = crate::ShardDirectory::new(vec![ShardEntry {
            shard: ShardNumber(0),
            range: KeyRange::<String> { start: None, end: None },
        }]);
        let router = Arc::new(DynamicRouter::new(Arc::new(RwLock::new(shard_dir))));

        Arc::new(BenchRoutingClient::new(tapir_client, router))
    }
}
