#![allow(dead_code, unused_imports)]

use super::workload::{prepopulate, workload_loop};
use super::{
    bootstrap_cluster, BenchConfig, BenchTarget, ClusterConfig, WorkloadConfig, WorkloadType,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct WorkloadCounters {
    label: String,
    attempted: Arc<AtomicU64>,
    committed: Arc<AtomicU64>,
}

pub async fn run_bench(target: &BenchTarget, config: &WorkloadConfig) {
    crate::testing::init_tracing();
    let mut rng = crate::Rng::from_seed(42);

    let client = target.create_client();
    eprintln!("prepopulating {} keys...", config.key_space_size);
    prepopulate(&client, config.key_space_size).await;
    eprintln!("prepopulate done, sleeping 1s for FINALIZE propagation");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut counters = Vec::new();
    let mut handles = Vec::new();

    for (workload_type, num_clients) in &config.workloads {
        let label = match workload_type {
            WorkloadType::ReadWrite { .. } => "RW",
            WorkloadType::ReadOnlyGet { .. } => "RO_GET",
            WorkloadType::ReadOnlyScan { .. } => "RO_SCAN",
        };
        let attempted = Arc::new(AtomicU64::new(0));
        let committed = Arc::new(AtomicU64::new(0));
        counters.push(WorkloadCounters {
            label: label.to_string(),
            attempted: Arc::clone(&attempted),
            committed: Arc::clone(&committed),
        });

        for _ in 0..*num_clients {
            let c = target.create_client();
            let wt = workload_type.clone();
            let ks = config.key_space_size;
            let att = Arc::clone(&attempted);
            let com = Arc::clone(&committed);
            let ms = config.max_sleep_ms;
            let task_rng = rng.fork();
            handles.push(tokio::spawn(async move {
                workload_loop(c, wt, ks, att, com, ms, task_rng).await;
            }));
        }
    }

    let duration = Duration::from_secs(config.duration_secs);
    let start = tokio::time::Instant::now();
    let mut last_report = start;
    let report_interval = Duration::from_secs(1);

    while start.elapsed() < duration {
        tokio::time::sleep(report_interval).await;
        let elapsed = last_report.elapsed().as_secs_f64();
        for c in &counters {
            let att = c.attempted.swap(0, Ordering::Relaxed);
            let com = c.committed.swap(0, Ordering::Relaxed);
            eprintln!(
                "TPUT {}: {:.0} attempted/s, {:.0} committed/s",
                c.label,
                att as f64 / elapsed,
                com as f64 / elapsed,
            );
        }
        last_report = tokio::time::Instant::now();
    }

    for h in handles {
        h.abort();
    }
}

pub async fn bootstrap_and_run_bench(config: BenchConfig) {
    let (target, _cluster) = bootstrap_cluster(&config.cluster).await;
    run_bench(&target, &config.workload).await;
}

pub async fn run_bench_auto(cluster_config: ClusterConfig, workload: WorkloadConfig) {
    if let Ok(addrs) = std::env::var("BENCH_CLUSTER") {
        let target = super::external_target(&addrs);
        run_bench(&target, &workload).await;
    } else {
        bootstrap_and_run_bench(BenchConfig {
            cluster: cluster_config,
            workload,
        })
        .await;
    }
}
