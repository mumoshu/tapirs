use super::*;

#[test]
fn bench_rw_smoke() {
    bench_runtime().block_on(async {
        runner::bootstrap_and_run_bench(BenchConfig {
            cluster: ClusterConfig {
                num_replicas: 3,
                linearizable: false,
            },
            workload: WorkloadConfig {
                key_space_size: 10,
                workloads: vec![(
                    WorkloadType::ReadWrite {
                        reads_per_txn: 1,
                        writes_per_txn: 1,
                    },
                    2,
                )],
                duration_secs: 2,
                max_sleep_ms: 5,
            },
        })
        .await;
    });
}
