#![allow(dead_code, unused_imports)]

use super::{BenchRoutingClient, WorkloadType};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub fn key(i: usize) -> String {
    format!("{:06}", i)
}

pub async fn prepopulate(client: &Arc<BenchRoutingClient>, key_space_size: usize) {
    let batch_size: usize = std::env::var("BENCH_PREPOPULATE_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let mut i = 0;
    while i < key_space_size {
        let end = (i + batch_size).min(key_space_size);
        let txn = client.begin();
        for j in i..end {
            // Read each key first (OCC requires read before write for new keys).
            let _ = txn.get(key(j)).await;
            txn.put(key(j), Some(format!("init_{j}")));
        }
        let result = txn.commit().await;
        if result.is_none() {
            // Retry this batch on OCC conflict.
            continue;
        }
        i = end;
    }
}

pub async fn workload_loop(
    client: Arc<BenchRoutingClient>,
    workload_type: WorkloadType,
    key_space_size: usize,
    attempted: Arc<AtomicU64>,
    committed: Arc<AtomicU64>,
    max_sleep_ms: u64,
    mut rng: crate::Rng,
) {
    loop {
        let ok = match &workload_type {
            WorkloadType::ReadWrite { reads_per_txn, writes_per_txn } => {
                let txn = client.begin();
                for _ in 0..*reads_per_txn {
                    let k = key(rng.random_index(key_space_size));
                    let _ = txn.get(k).await;
                }
                for _ in 0..*writes_per_txn {
                    let k = key(rng.random_index(key_space_size));
                    txn.put(k, Some(format!("{}", rng.random_u64())));
                }
                txn.commit().await.is_some()
            }
            _ => unreachable!("workload type not yet implemented"),
        };
        attempted.fetch_add(1, Ordering::Relaxed);
        committed.fetch_add(ok as u64, Ordering::Relaxed);
        let sleep_ms = (rng.random_u64() % max_sleep_ms).max(1);
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
}
