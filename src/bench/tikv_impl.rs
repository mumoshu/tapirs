#![allow(dead_code)]

use super::ops::{Op, TxnOps};
use super::traits::{BenchWorkload, TargetResolver};
use std::sync::Arc;
use tikv_client::TransactionClient;

pub struct TikvTarget {
    pub client: Arc<TransactionClient>,
}

pub struct TikvResolver;

impl TargetResolver for TikvResolver {
    type Target = TikvTarget;

    fn resolve(connection_str: &str) -> TikvTarget {
        let pd_addrs: Vec<String> = connection_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        let client = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                TransactionClient::new(pd_addrs)
                    .await
                    .expect("failed to connect to TiKV PD")
            })
        });
        TikvTarget { client: Arc::new(client) }
    }
}

pub struct TikvWorkload;

impl BenchWorkload<TikvTarget> for TikvWorkload {
    type Client = TransactionClient;

    fn create_client(target: &TikvTarget) -> Arc<TransactionClient> {
        Arc::clone(&target.client)
    }

    async fn execute_txn(client: &TransactionClient, ops: TxnOps) -> bool {
        execute_tikv_txn(client, ops).await.is_ok()
    }

    async fn prepopulate(client: &TransactionClient, batches: Vec<TxnOps>) {
        for batch in batches {
            loop {
                match execute_tikv_txn(client, batch.clone()).await {
                    Ok(_) => break,
                    Err(e) => {
                        eprintln!("prepopulate batch failed, retrying: {e}");
                    }
                }
            }
        }
    }
}

async fn execute_tikv_txn(
    client: &TransactionClient,
    ops: TxnOps,
) -> Result<(), tikv_client::Error> {
    let mut txn = client.begin_optimistic().await?;
    for op in ops.ops {
        match op {
            Op::Get { key } => {
                let _ = txn.get(key).await?;
            }
            Op::Put { key, value } => {
                txn.put(key, value).await?;
            }
            Op::Scan { start, end } => {
                let _ = txn.scan(start..end, 10000).await?;
            }
        }
    }
    if !ops.read_only {
        txn.commit().await?;
    }
    Ok(())
}
