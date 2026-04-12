#![allow(clippy::type_complexity)]

pub mod backup;
pub mod discovery;
mod ir;
pub mod storage;
pub mod node;
pub mod nodecluster;
mod occ;
mod rng;
pub mod sharding;
mod tapir;

#[cfg(feature = "tls")]
pub mod tls;
mod transport;
pub mod util;


#[cfg(test)]
pub mod testing;

#[cfg(test)]
mod bench;

pub use rng::Rng;

pub use ir::{
    Client as IrClient, ClientConfig as IrClientConfig, ClientId as IrClientId, Membership as IrMembership,
    MembershipSize as IrMembershipSize, Message as IrMessage, OpId as IrOpId, Record as IrRecord,
    IrPayload, IrRecordStore, RecordIter as IrRecordIter, RecordView as IrRecordView,
    RecordConsensusEntry as IrRecordConsensusEntry, Replica as IrReplica,
    ReplicaMetrics as IrReplicaMetrics, ReplicaUpcalls as IrReplicaUpcalls,
    SharedView as IrSharedView, View as IrView, ViewNumber as IrViewNumber,
};
pub use storage::io::StorageError;
pub use storage::io::disk_io::{BufferedIo, OpenFlags as DiskOpenFlags};
pub use storage::io::memory_io::MemoryIo;
#[cfg(test)]
pub use storage::io::memory_io::MemoryIo as DefaultDiskIo;
#[cfg(not(test))]
pub use storage::io::disk_io::BufferedIo as DefaultDiskIo;
pub use occ::{
    PrepareResult as OccPrepareResult, ScanEntry as OccScanEntry,
    Timestamp as OccTimestamp, SharedTransaction as OccSharedTransaction,
    Transaction as OccTransaction, TransactionId as OccTransactionId,
};
pub use tapir::{
    Client as TapirClient, KeyRange, ReadOnlyTransaction, Replica as TapirReplica,
    RoutingClient, RoutingReadOnlyTransaction, RoutingReadReplicaTransaction, RoutingTimeTravelTransaction, RoutingTransaction,
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
