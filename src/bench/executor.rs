use super::ops::{Op, TxnOps};
use super::BenchRoutingClient;
use std::time::Duration;

/// Execute a single transaction's operations against a TAPIR cluster.
pub async fn execute_txn(client: &BenchRoutingClient, txn_ops: TxnOps) -> bool {
    if txn_ops.read_only {
        let txn = client.begin_read_only(Duration::ZERO);
        for op in txn_ops.ops {
            match op {
                Op::Get { key } => {
                    let _ = txn.get(key).await;
                }
                Op::Scan { start, end } => {
                    let _ = txn.scan(start, end).await;
                }
                Op::Put { .. } => unreachable!("put in read-only txn"),
            }
        }
        true
    } else {
        let txn = client.begin();
        for op in txn_ops.ops {
            match op {
                Op::Get { key } => {
                    let _ = txn.get(key).await;
                }
                Op::Put { key, value } => {
                    txn.put(key, Some(value));
                }
                Op::Scan { start, end } => {
                    let _ = txn.scan(start, end).await;
                }
            }
        }
        txn.commit().await.is_some()
    }
}

/// Execute prepopulation: run each batch as a RW transaction, retrying on conflict.
pub async fn prepopulate(client: &BenchRoutingClient, batches: Vec<TxnOps>) {
    for batch in batches {
        loop {
            let txn = client.begin();
            for op in &batch.ops {
                match op {
                    Op::Get { key } => {
                        let _ = txn.get(key.clone()).await;
                    }
                    Op::Put { key, value } => {
                        txn.put(key.clone(), Some(value.clone()));
                    }
                    Op::Scan { .. } => unreachable!("scan in prepopulate"),
                }
            }
            if txn.commit().await.is_some() {
                break;
            }
        }
    }
}
