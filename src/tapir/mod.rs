mod client;
pub mod dns_shard_client;
pub mod dynamic_router;
pub mod key_range;
mod key_value;
mod message;
mod quorum_read;
pub(crate) mod replica;
mod routing_client;
mod shard_client;
pub mod shard_router;
mod timestamp;
pub mod timetravel;

mod shard;
#[cfg(test)]
mod tests;

pub use client::{Client, ReadOnlyTransaction};
pub use timetravel::{RoutingTimeTravelTransaction, TimeTravelTransaction};
pub use key_range::KeyRange;
pub use key_value::{Key, Value};
pub use message::{Change, LeaderRecordDelta, CO, CR, IO, IR, UO, UR};
pub use replica::Replica;
pub use routing_client::{RoutingClient, RoutingReadOnlyTransaction, RoutingTransaction};
pub use shard::{Number as ShardNumber, Sharded};
pub use shard_client::{ScanChangesResult, ShardClient};
pub use timestamp::Timestamp;

/// Errors that can occur during transaction operations.
#[derive(Debug, Clone)]
pub enum TransactionError {
    /// The key is outside the shard's current range. This indicates
    /// the routing directory is stale — the shard was re-ranged during
    /// resharding. The caller should refresh routing and retry.
    OutOfRange,
    /// The shard's replicas are temporarily unreachable (e.g. view change
    /// in progress, network partition). The caller should retry later.
    Unavailable,
    /// A prepared-but-uncommitted write overlaps the read's snapshot
    /// timestamp. The caller should retry with backoff — the prepare
    /// will resolve (commit or abort) shortly.
    PrepareConflict,
}
