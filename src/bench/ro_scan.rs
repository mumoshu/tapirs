use super::*;

#[test]
fn bench_ro_scan_smoke() {
    bench_runtime().block_on(async {
        runner::bootstrap_and_run_bench(BenchConfig {
            cluster: ClusterConfig {
                num_replicas: 3,
                linearizable: false,
            },
            workload: WorkloadConfig {
                key_space_size: 20,
                workloads: vec![(
                    WorkloadType::ReadOnlyScan { scan_range_size: 5 },
                    2,
                )],
                duration_secs: 2,
                max_sleep_ms: 5,
            },
        })
        .await;
    });
}
