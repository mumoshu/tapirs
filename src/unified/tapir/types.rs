use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;

/// Deserialized committed-transaction payload with typed keys and values.
///
/// TAPIR owns this type because its fields encode TAPIR/OCC transaction
/// semantics (`transaction_id`, `commit_ts`, read/write/scan sets).
pub struct Transaction<K, V> {
    pub transaction_id: OccTransactionId,
    /// Prepare-time (proposed) commit timestamp.
    ///
    /// This is the timestamp the client proposed at prepare time, NOT
    /// necessarily the final commit timestamp. In TAPIR, replicas may
    /// return `Retry { proposed }` with a higher timestamp, and the
    /// coordinator picks the maximum as the final commit timestamp.
    /// The final timestamp is passed separately to commit paths.
    pub commit_ts: Timestamp,
    pub read_set: Vec<(K, Timestamp)>,
    pub write_set: Vec<(K, Option<V>)>,
    #[allow(dead_code)]
    pub scan_set: Vec<(K, K, Timestamp)>,
}
