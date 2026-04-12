#![allow(unused_imports)]

use super::ops::TxnOps;
use super::traits::BenchWorkload;
use super::workload_gen::WorkloadGen;
use super::WorkloadType;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub async fn prepopulate<T, W>(client: &Arc<W::Client>, key_space_size: usize)
where
    T: Send + Sync + 'static,
    W: BenchWorkload<T>,
{
    let batches = super::workload_gen::prepopulate_ops(key_space_size);
    W::prepopulate(client, batches).await;
}

pub async fn workload_loop<T, W>(
    client: Arc<W::Client>,
    workload_type: WorkloadType,
    key_space_size: usize,
    attempted: Arc<AtomicU64>,
    committed: Arc<AtomicU64>,
    max_sleep_ms: u64,
    mut rng: crate::Rng,
) where
    T: Send + Sync + 'static,
    W: BenchWorkload<T>,
{
    let gen_rng = rng.fork();
    let mut wgen = WorkloadGen::new(gen_rng, workload_type, key_space_size);
    loop {
        let ops = wgen.next().unwrap();
        let ok = W::execute_txn(&client, ops).await;
        attempted.fetch_add(1, Ordering::Relaxed);
        committed.fetch_add(ok as u64, Ordering::Relaxed);
        let sleep_ms = (rng.random_u64() % max_sleep_ms).max(1);
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
}
