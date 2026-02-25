#![allow(dead_code, unused_imports)]

mod cluster;
mod executor;
mod workload_gen;
mod mixed;
mod ops;
mod ro_get;
mod ro_scan;
mod runner;
mod rw;
mod workload;

pub use cluster::{bootstrap_cluster, external_target, BenchCluster, BenchTarget};

use crate::{DynamicRouter, RoutingClient, TapirClient, TapirReplica, TcpTransport};

pub type K = String;
pub type V = String;
pub type BenchTransport = TcpTransport<TapirReplica<K, V>>;
pub type BenchTapirClient = TapirClient<K, V, BenchTransport>;
pub type BenchRoutingClient = RoutingClient<K, V, BenchTransport, DynamicRouter<K>>;

pub struct ClusterConfig {
    pub num_replicas: usize,
    pub linearizable: bool,
}

pub struct WorkloadConfig {
    pub key_space_size: usize,
    /// (workload_type, num_clients) pairs.
    pub workloads: Vec<(WorkloadType, usize)>,
    pub duration_secs: u64,
    pub max_sleep_ms: u64,
}

pub struct BenchConfig {
    pub cluster: ClusterConfig,
    pub workload: WorkloadConfig,
}

#[derive(Clone)]
#[allow(clippy::enum_variant_names)]
pub enum WorkloadType {
    ReadWrite {
        reads_per_txn: usize,
        writes_per_txn: usize,
    },
    ReadOnlyGet {
        reads_per_txn: usize,
    },
    ReadOnlyScan {
        scan_range_size: usize,
    },
}

pub fn bench_runtime() -> tokio::runtime::Runtime {
    let threads: usize = std::env::var("BENCH_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .expect("failed to build bench runtime")
}
