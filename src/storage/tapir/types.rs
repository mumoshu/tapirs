use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Entry in the committed VlogLsm transaction log.
/// Committed entries contain the full transaction; aborted entries only
/// preserve the timestamp (for idempotent abort responses).
#[derive(Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: serde::de::DeserializeOwned, V: serde::de::DeserializeOwned"
))]
pub(crate) enum TxnLogEntry<K, V> {
    Committed(Arc<Transaction<K, V>>),
    Aborted(Timestamp),
}

/// Deserialized transaction payload with typed keys and values.
///
/// TAPIR owns this type because its fields encode TAPIR/OCC transaction
/// semantics (`transaction_id`, `commit_ts`, read/write/scan sets).
///
/// Stored in two VlogLsm instances with different semantics:
/// - **Prepared VlogLsm**: `commit_ts` is the proposed commit timestamp
///   from the client. Replicas may return `Retry { proposed }` with a
///   higher timestamp; the coordinator picks the maximum as the final
///   commit timestamp.
/// - **Committed VlogLsm**: `commit_ts` is the final commit timestamp
///   agreed upon by the coordinator.
#[derive(Serialize, Deserialize)]
pub(crate) struct Transaction<K, V> {
    pub transaction_id: OccTransactionId,
    /// Commit timestamp. In the prepared VlogLsm this is the proposed
    /// timestamp; in the committed VlogLsm this is the final timestamp.
    pub commit_ts: Timestamp,
    pub read_set: Vec<(K, Timestamp)>,
    pub write_set: Vec<(K, Option<V>)>,
    pub scan_set: Vec<(K, K, Timestamp)>,
}
