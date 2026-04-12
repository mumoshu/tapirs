use super::ops::TxnOps;
use std::future::Future;
use std::sync::Arc;

/// Resolves a connection string into target-specific config.
pub trait TargetResolver {
    type Target: Send + Sync + 'static;
    fn resolve(connection_str: &str) -> Self::Target;
}

/// Abstracts client creation and transaction execution for a database.
pub trait BenchWorkload<T: Send + Sync + 'static> {
    type Client: Send + Sync + 'static;

    fn create_client(target: &T) -> Arc<Self::Client>;

    /// Execute a single transaction. Returns true if committed successfully.
    fn execute_txn(
        client: &Self::Client,
        ops: TxnOps,
    ) -> impl Future<Output = bool> + Send;

    /// Prepopulate the database with initial data.
    fn prepopulate(
        client: &Self::Client,
        ops: Vec<TxnOps>,
    ) -> impl Future<Output = ()> + Send;
}
