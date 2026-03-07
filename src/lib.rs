#![allow(clippy::type_complexity)]

pub mod backup;
pub mod discovery;
mod ir;
mod mvcc;
pub mod node;
pub mod nodecluster;
mod occ;
mod rng;
pub mod sharding;
mod tapir;
pub mod tapirstore;
#[cfg(feature = "tls")]
pub mod tls;
mod transport;
pub mod unified;
pub mod util;


#[cfg(test)]
pub mod testing;

#[cfg(test)]
mod bench;

pub use rng::Rng;

pub use ir::{
    Client as IrClient, ClientId as IrClientId, Membership as IrMembership,
    MembershipSize as IrMembershipSize, Message as IrMessage, OpId as IrOpId, Record as IrRecord,
    IrRecordStore,
    RecordConsensusEntry as IrRecordConsensusEntry, Replica as IrReplica,
    ReplicaMetrics as IrReplicaMetrics, ReplicaUpcalls as IrReplicaUpcalls,
    VersionedRecord as IrVersionedRecord,
    SharedView as IrSharedView, View as IrView, ViewNumber as IrViewNumber,
};
pub use mvcc::MvccBackend;
#[cfg(feature = "surrealkv")]
pub use mvcc::surrealkvstore::SurrealKvStore;
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
    RoutingClient, RoutingReadOnlyTransaction, RoutingTimeTravelTransaction, RoutingTransaction,
    ScanChangesResult, Sharded, ShardNumber, Timestamp as TapirTimestamp, TimeTravelTransaction,
    TransactionError,
    CO as TapirCO, CR as TapirCR, IO as TapirIO,
};
pub use nodecluster::{CloneError, SoloClusterManager};
pub use tapir::dns_shard_client::DnsRefreshingShardClient;
pub use tapir::dynamic_router::{DynamicRouter, ShardDirectory, ShardEntry};
pub use sharding::shardmanager::ShardManager;
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
