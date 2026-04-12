use super::ops::{Op, TxnOps};
use super::traits::{BenchWorkload, TargetResolver};
use std::sync::Arc;
use tikv_client::TransactionClient;

pub(crate) struct TikvTarget {
    pub(crate) client: Arc<TransactionClient>,
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
            let mut attempts = 0;
            loop {
                match execute_tikv_txn(client, batch.clone()).await {
                    Ok(_) => break,
                    Err(e) => {
                        attempts += 1;
                        if attempts >= 50 {
                            panic!("prepopulate batch failed after {attempts} attempts: {e}");
                        }
                        if attempts % 10 == 1 {
                            eprintln!("prepopulate batch attempt {attempts} failed: {e}");
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
    let result = execute_ops(&mut txn, &ops).await;
    if result.is_err() || ops.read_only {
        // Rollback on error or for read-only transactions.
        // Ignore rollback errors — the transaction may already be aborted.
        let _ = txn.rollback().await;
        return result;
    }
    txn.commit().await?;
    Ok(())
}

async fn execute_ops(
    txn: &mut tikv_client::Transaction,
    ops: &TxnOps,
) -> Result<(), tikv_client::Error> {
    for op in &ops.ops {
        match op {
            Op::Get { key } => {
                let _ = txn.get(key.clone()).await?;
            }
            Op::Put { key, value } => {
                txn.put(key.clone(), value.clone()).await?;
            }
            Op::Scan { start, end } => {
                let _ = txn.scan(start.clone()..end.clone(), 10000).await?;
            }
        }
    }
    Ok(())
}
