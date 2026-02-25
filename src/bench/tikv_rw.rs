#![cfg(feature = "tikv")]

use super::*;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Run TiKV bench against external cluster (BENCH_TIKV env var).
/// Skips if BENCH_TIKV is not set.
fn run_tikv_bench(default_workload: WorkloadConfig) {
    let pd_addr = match std::env::var("BENCH_TIKV") {
        Ok(addr) => addr,
        Err(_) => {
            eprintln!("BENCH_TIKV not set, skipping TiKV benchmark");
            return;
        }
    };
    let workload = WorkloadConfig {
        key_space_size: env_usize("BENCH_KEY_SPACE", default_workload.key_space_size),
        duration_secs: env_u64("BENCH_DURATION", default_workload.duration_secs),
        workloads: default_workload
            .workloads
            .into_iter()
            .map(|(wt, default_n)| (wt, env_usize("BENCH_CLIENTS", default_n)))
            .collect(),
        max_sleep_ms: default_workload.max_sleep_ms,
    };
    bench_runtime().block_on(async {
        runner::run_bench::<tikv_impl::TikvResolver, tikv_impl::TikvWorkload>(&pd_addr, &workload)
            .await;
    });
}

/// Functional smoke test: 2 clients, 2s, small key space.
#[test]
#[ignore]
fn bench_tikv_rw_smoke() {
    run_tikv_bench(WorkloadConfig {
        key_space_size: 20,
        workloads: vec![(
            WorkloadType::ReadWrite {
                reads_per_txn: 1,
                writes_per_txn: 1,
            },
            2,
        )],
        duration_secs: 2,
        max_sleep_ms: 5,
    });
}

/// Full benchmark: 100 clients, 10s, 10k keys.
#[test]
#[ignore]
fn bench_tikv_rw() {
    run_tikv_bench(WorkloadConfig {
        key_space_size: 10_000,
        workloads: vec![(
            WorkloadType::ReadWrite {
                reads_per_txn: 1,
                writes_per_txn: 1,
            },
            100,
        )],
        duration_secs: 10,
        max_sleep_ms: 5,
    });
}

/// Full mixed benchmark.
#[test]
#[ignore]
fn bench_tikv_mixed() {
    run_tikv_bench(WorkloadConfig {
        key_space_size: 10_000,
        workloads: vec![
            (WorkloadType::ReadOnlyGet { reads_per_txn: 2 }, 50),
            (
                WorkloadType::ReadOnlyScan {
                    scan_range_size: 100,
                },
                10,
            ),
            (
                WorkloadType::ReadWrite {
                    reads_per_txn: 1,
                    writes_per_txn: 1,
                },
                100,
            ),
        ],
        duration_secs: 10,
        max_sleep_ms: 5,
    });
}
