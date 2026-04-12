//! Configurable benchmark framework for transactional KV stores.
//!
//! The framework separates workload generation from execution so the same
//! workloads (RW, RO GET, RO SCAN, mixed) can run against any database.
//!
//! # Running TAPIR benchmarks
//!
//! Smoke tests (included in `make test`, ~3s each):
//!
//! ```bash
//! cargo test bench_rw_smoke --release -- --nocapture
//! cargo test bench_ro --release -- --nocapture
//! cargo test bench_mixed --release -- --nocapture
//! ```
//!
//! Full benchmarks (10s, hundreds of clients):
//!
//! ```bash
//! make bench/rw   # 1000 RW clients, 10k keys
//! make bench/ro   # 500 RO GET + 500 RO SCAN clients
//! make bench/mix  # 400 RO GET + 100 RO SCAN + 500 RW clients
//! ```
//!
//! Against an external cluster (e.g. testbed-solo):
//!
//! ```bash
//! BENCH_CLUSTER=127.0.0.1:6000,127.0.0.1:6001,127.0.0.1:6002 \
//!   cargo test bench_rw --release -- --nocapture --include-ignored
//! ```
//!
//! # Adding a new target
//!
//! Implement [`traits::TargetResolver`] and [`traits::BenchWorkload`], then
//! call [`runner::run_bench`] with your types. Workload generation
//! ([`workload_gen::WorkloadGen`]) produces target-agnostic [`ops::TxnOps`],
//! so only the execution layer needs a new implementation.
//!
//! ```rust,ignore
//! struct PgResolver;
//! impl TargetResolver for PgResolver {
//!     type Target = PgPool;
//!     fn resolve(connection_str: &str) -> PgPool {
//!         PgPool::connect(connection_str)
//!     }
//! }
//!
//! struct PgWorkload;
//! impl BenchWorkload<PgPool> for PgWorkload {
//!     type Client = PgClient;
//!     fn create_client(target: &PgPool) -> Arc<PgClient> { /* ... */ }
//!     async fn execute_txn(client: &PgClient, ops: TxnOps) -> bool { /* ... */ }
//!     async fn prepopulate(client: &PgClient, ops: Vec<TxnOps>) { /* ... */ }
//! }
//!
//! // Then run:
//! run_bench::<PgResolver, PgWorkload>("postgresql://localhost/bench", &config).await;
//! ```

#![allow(unused_imports)]

mod cluster;
mod executor;
mod mixed;
mod ops;
mod ro_get;
mod ro_scan;
mod runner;
mod rw;
mod tapir_impl;
#[cfg(feature = "tikv")]
mod tikv_impl;
#[cfg(feature = "tikv")]
mod tikv_rw;
mod traits;
mod workload;
mod workload_gen;

pub(crate) use cluster::{bootstrap_cluster, external_target, BenchCluster, BenchTarget};

use crate::{DynamicRouter, RoutingClient, TapirClient, TapirReplica, TcpTransport};

pub type K = String;
pub type V = String;
pub type BenchTransport = TcpTransport<TapirReplica<K, V>>;
pub type BenchTapirClient = TapirClient<K, V, BenchTransport>;
pub type BenchRoutingClient = RoutingClient<K, V, BenchTransport, DynamicRouter<K>>;

pub(crate) struct ClusterConfig {
    pub(crate) num_replicas: usize,
}

pub(crate) struct WorkloadConfig {
    pub(crate) key_space_size: usize,
    /// (workload_type, num_clients) pairs.
    pub(crate) workloads: Vec<(WorkloadType, usize)>,
    pub(crate) duration_secs: u64,
    pub(crate) max_sleep_ms: u64,
}

pub(crate) struct BenchConfig {
    pub(crate) cluster: ClusterConfig,
    pub(crate) workload: WorkloadConfig,
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
