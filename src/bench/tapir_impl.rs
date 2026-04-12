use super::traits::{BenchWorkload, TargetResolver};
use super::{executor, ops::TxnOps, BenchRoutingClient, BenchTarget};
use std::sync::Arc;

pub struct TapirResolver;

impl TargetResolver for TapirResolver {
    type Target = BenchTarget;
    fn resolve(connection_str: &str) -> BenchTarget {
        super::external_target(connection_str)
    }
}

pub struct TapirWorkload;

impl BenchWorkload<BenchTarget> for TapirWorkload {
    type Client = BenchRoutingClient;

    fn create_client(target: &BenchTarget) -> Arc<BenchRoutingClient> {
        target.create_client()
    }

    async fn execute_txn(client: &BenchRoutingClient, ops: TxnOps) -> bool {
        executor::execute_txn(client, ops).await
    }

    async fn prepopulate(client: &BenchRoutingClient, ops: Vec<TxnOps>) {
        executor::prepopulate(client, ops).await
    }
}
