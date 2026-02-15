#![allow(clippy::type_complexity)]

pub mod discovery;
mod ir;
mod mvcc;
mod occ;
mod tapir;
mod transport;
pub mod util;

pub use ir::{
    Client as IrClient, ClientId as IrClientId, Membership as IrMembership,
    MembershipSize as IrMembershipSize, Message as IrMessage, OpId as IrOpId, Record as IrRecord,
    RecordConsensusEntry as IrRecordConsensusEntry, Replica as IrReplica,
    ReplicaUpcalls as IrReplicaUpcalls, SharedView as IrSharedView,
};
pub use mvcc::{MemoryStore as MvccMemoryStore, MvccBackend, Store as MvccStore};
pub use mvcc::disk::{DiskStore as MvccDiskStore, StorageError};
pub use occ::{
    PrepareResult as OccPrepareResult, ScanEntry as OccScanEntry, Store as OccStore,
    Timestamp as OccTimestamp, SharedTransaction as OccSharedTransaction,
    Transaction as OccTransaction, TransactionId as OccTransactionId,
};
pub use tapir::{
    Client as TapirClient, KeyRange, Replica as TapirReplica, RoutingClient, RoutingTransaction,
    Sharded, ShardNumber, Timestamp as TapirTimestamp,
};
pub use tapir::dynamic_router::{DynamicRouter, ShardDirectory, ShardEntry};
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
