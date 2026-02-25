use super::*;

#[test]
fn bench_ro_get_smoke() {
    bench_runtime().block_on(async {
        runner::bootstrap_and_run_bench(BenchConfig {
            cluster: ClusterConfig {
                num_replicas: 3,
                linearizable: false,
            },
            workload: WorkloadConfig {
                key_space_size: 10,
                workloads: vec![(
                    WorkloadType::ReadOnlyGet { reads_per_txn: 2 },
                    2,
                )],
                duration_secs: 2,
                max_sleep_ms: 5,
            },
        })
        .await;
    });
}

#[test]
#[ignore]
fn bench_ro_get() {
    bench_runtime().block_on(async {
        runner::run_bench_auto(
            ClusterConfig {
                num_replicas: 3,
                linearizable: false,
            },
            WorkloadConfig {
                key_space_size: 10_000,
                workloads: vec![(
                    WorkloadType::ReadOnlyGet { reads_per_txn: 2 },
                    500,
                )],
                duration_secs: 10,
                max_sleep_ms: 5,
            },
        )
        .await;
    });
}
