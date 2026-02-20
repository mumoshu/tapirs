mod client;
pub mod dns_shard_client;
pub mod dynamic_router;
pub mod key_range;
mod key_value;
mod message;
mod replica;
mod routing_client;
#[allow(dead_code)]
mod shard_client;
#[allow(dead_code)]
pub mod shard_manager;
#[allow(dead_code)]
mod shard_manager_catchup;
#[allow(dead_code)]
mod shard_manager_cdc;
pub mod shard_router;
mod timestamp;

mod shard;
#[cfg(test)]
mod tests;

pub use client::{Client, ReadOnlyTransaction};
pub use key_range::KeyRange;
pub use key_value::{Key, Value};
pub use message::{Change, LeaderRecordDelta, CO, CR, IO, IR, UO, UR};
pub use replica::Replica;
pub use routing_client::{RoutingClient, RoutingReadOnlyTransaction, RoutingTransaction};
pub use shard::{Number as ShardNumber, Sharded};
pub use shard_client::ShardClient;
pub use timestamp::Timestamp;

/// Errors that can occur during transaction operations.
#[derive(Debug, Clone)]
pub enum TransactionError {
    /// The key is outside the shard's current range. This indicates
    /// the routing directory is stale — the shard was re-ranged during
    /// resharding. The caller should refresh routing and retry.
    OutOfRange,
}
