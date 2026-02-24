#![allow(clippy::type_complexity)]

pub mod discovery;
mod ir;
mod mvcc;
pub mod node;
mod occ;
mod rng;
pub mod shard_manager_api;
mod tapir;
#[cfg(feature = "tls")]
pub mod tls;
mod transport;
pub mod util;

#[cfg(test)]
pub mod testing;

pub use rng::Rng;

pub use ir::{
    Client as IrClient, ClientId as IrClientId, Membership as IrMembership,
    MembershipSize as IrMembershipSize, Message as IrMessage, OpId as IrOpId, Record as IrRecord,
    RecordConsensusEntry as IrRecordConsensusEntry, Replica as IrReplica,
    ReplicaMetrics as IrReplicaMetrics, ReplicaUpcalls as IrReplicaUpcalls,
    SharedView as IrSharedView, View as IrView, ViewNumber as IrViewNumber,
};
pub use mvcc::MvccBackend;
pub use mvcc::disk::{DiskStore as MvccDiskStore, StorageError};
pub use mvcc::disk::disk_io::{BufferedIo, OpenFlags as DiskOpenFlags};
#[cfg(test)]
pub use mvcc::disk::memory_io::MemoryIo as DefaultDiskIo;
#[cfg(all(not(test), target_os = "linux", feature = "io-uring"))]
pub use transport::uring::UringDirectIo as DefaultDiskIo;
#[cfg(all(not(test), not(all(target_os = "linux", feature = "io-uring"))))]
pub use mvcc::disk::disk_io::BufferedIo as DefaultDiskIo;
pub use occ::{
    PrepareResult as OccPrepareResult, ScanEntry as OccScanEntry, Store as OccStore,
    Timestamp as OccTimestamp, SharedTransaction as OccSharedTransaction,
    Transaction as OccTransaction, TransactionId as OccTransactionId,
};
pub use tapir::{
    Client as TapirClient, KeyRange, ReadOnlyTransaction, Replica as TapirReplica,
    RoutingClient, RoutingReadOnlyTransaction, RoutingTransaction,
    Sharded, ShardNumber, Timestamp as TapirTimestamp, TransactionError,
};
pub use tapir::clustermanager::{CloneError, SoloClusterManager};
pub use tapir::dns_shard_client::DnsRefreshingShardClient;
pub use tapir::dynamic_router::{DynamicRouter, ShardDirectory, ShardEntry};
pub use tapir::shard_manager::ShardManager;
pub use transport::{
    Channel as ChannelTransport, ChannelRegistry, Message as TransportMessage, TapirTransport,
    Transport,
};
pub use transport::tokio_bitcode_tcp::{TcpAddress, TcpTransport};

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use transport::uring::{
    CoreConfig, CoreLauncher, ShardAssignment, UringAddress, UringDirectIo, UringError,
    UringSleep, UringTransport,
};
