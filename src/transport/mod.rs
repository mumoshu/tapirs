use crate::{
    tapir::{Key, Value},
    tapirstore::TapirStore,
    IrMembership, IrMessage, IrReplicaUpcalls, ShardNumber,
};
pub use channel::{Channel, Registry as ChannelRegistry};
pub use message::Message;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fmt::{Debug, Display},
    future::Future,
    hash::Hash,
    time::{Duration, SystemTime},
};

mod channel;
mod message;

#[cfg(test)]
pub use faulty_channel::{FaultyChannelTransport, NetworkFaultConfig, LatencyConfig};
#[cfg(test)]
mod faulty_channel;

pub mod tokio_bitcode_tcp;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod uring;

pub trait Transport<U: IrReplicaUpcalls>: Clone + Send + Sync + 'static {
    type Address: Copy + Ord + Eq + Hash + Debug + Display + Send + Sync + Serialize + DeserializeOwned + 'static;
    type Sleep: Future<Output = ()> + Send;

    /// Get own address.
    fn address(&self) -> Self::Address;

    /// Get time (nanos since epoch).
    fn time(&self) -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    fn time_offset(&self, offset: i64) -> u64 {
        self.time()
            .saturating_add_signed(offset.saturating_mul(1000 * 1000))
    }

    /// Sleep for duration.
    fn sleep(duration: Duration) -> Self::Sleep;

    /// Send/retry, ignoring any errors, until there is a reply.
    fn send<R: TryFrom<IrMessage<U, Self>> + Send + Debug>(
        &self,
        address: Self::Address,
        message: impl Into<IrMessage<U, Self>> + Debug,
    ) -> impl Future<Output = R> + Send + 'static;

    /// Send once and don't wait for a reply.
    fn do_send(&self, address: Self::Address, message: impl Into<IrMessage<U, Self>> + Debug);

    /// Spawn a fire-and-forget async task.
    fn spawn(future: impl Future<Output = ()> + Send + 'static);

    /// Called after a view change completes with the new group membership.
    /// Default: no-op.
    fn on_membership_changed(&self, _membership: &IrMembership<Self::Address>, _view: u64) {}
}

pub trait TapirTransport<K: Key, V: Value, S: TapirStore<K, V> = crate::tapirstore::InMemTapirStore<K, V, crate::MvccDiskStore<K, V, crate::tapir::Timestamp, crate::DefaultDiskIo>>>: Transport<crate::tapir::Replica<K, V, S>> {
    /// Look up the addresses of replicas in a shard, on a best-effort basis; results
    /// may be arbitrarily out of date.
    fn shard_addresses(
        &self,
        shard: ShardNumber,
    ) -> impl Future<Output = IrMembership<Self::Address>> + Send + 'static;

}
