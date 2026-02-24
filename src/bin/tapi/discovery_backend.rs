use tapirs::discovery::json::JsonRemoteShardDirectory;
use tapirs::discovery::tapir::TapirRemoteShardDirectory;
use tapirs::discovery::{DiscoveryError, RemoteShardDirectory, ShardDirectoryChange, ShardDirectoryChangeSet};
use tapirs::{IrMembership, ShardNumber, TapirReplica, TcpAddress, TcpTransport};

type DiscoveryTapirDir =
    TapirRemoteShardDirectory<TcpAddress, TcpTransport<TapirReplica<String, String>>>;

/// Dispatch enum for [`RemoteShardDirectory<TcpAddress>`] backends.
///
/// `RemoteShardDirectory` uses RPITIT (not object-safe), so a trait object
/// cannot be used. This enum wraps the concrete backend types and delegates
/// each method to the active variant.
pub enum DiscoveryBackend {
    Json(JsonRemoteShardDirectory<TcpAddress>),
    Tapir(DiscoveryTapirDir),
}

impl RemoteShardDirectory<TcpAddress, String> for DiscoveryBackend {
    async fn weak_get(
        &self,
        shard: ShardNumber,
    ) -> Result<Option<(IrMembership<TcpAddress>, u64)>, DiscoveryError> {
        match self {
            Self::Json(c) => {
                <JsonRemoteShardDirectory<TcpAddress> as RemoteShardDirectory<TcpAddress, String>>::weak_get(c, shard)
                    .await
            }
            Self::Tapir(c) => {
                <DiscoveryTapirDir as RemoteShardDirectory<TcpAddress, String>>::weak_get(c, shard)
                    .await
            }
        }
    }

    async fn strong_put(
        &self,
        shard: ShardNumber,
        membership: IrMembership<TcpAddress>,
        view: u64,
    ) -> Result<(), DiscoveryError> {
        match self {
            Self::Json(c) => {
                <JsonRemoteShardDirectory<TcpAddress> as RemoteShardDirectory<TcpAddress, String>>::strong_put(
                    c, shard, membership, view,
                )
                .await
            }
            Self::Tapir(c) => {
                <DiscoveryTapirDir as RemoteShardDirectory<TcpAddress, String>>::strong_put(
                    c, shard, membership, view,
                )
                .await
            }
        }
    }

    async fn strong_remove(&self, shard: ShardNumber) -> Result<(), DiscoveryError> {
        match self {
            Self::Json(c) => {
                <JsonRemoteShardDirectory<TcpAddress> as RemoteShardDirectory<TcpAddress, String>>::strong_remove(c, shard)
                    .await
            }
            Self::Tapir(c) => {
                <DiscoveryTapirDir as RemoteShardDirectory<TcpAddress, String>>::strong_remove(c, shard)
                    .await
            }
        }
    }

    async fn weak_all(
        &self,
    ) -> Result<Vec<(ShardNumber, IrMembership<TcpAddress>, u64)>, DiscoveryError> {
        match self {
            Self::Json(c) => {
                <JsonRemoteShardDirectory<TcpAddress> as RemoteShardDirectory<TcpAddress, String>>::weak_all(c).await
            }
            Self::Tapir(c) => {
                <DiscoveryTapirDir as RemoteShardDirectory<TcpAddress, String>>::weak_all(c).await
            }
        }
    }

    async fn strong_replace(
        &self,
        old: ShardNumber,
        new: ShardNumber,
        membership: IrMembership<TcpAddress>,
        view: u64,
    ) -> Result<(), DiscoveryError> {
        match self {
            Self::Json(c) => {
                <JsonRemoteShardDirectory<TcpAddress> as RemoteShardDirectory<TcpAddress, String>>::strong_replace(
                    c, old, new, membership, view,
                )
                .await
            }
            Self::Tapir(c) => {
                <DiscoveryTapirDir as RemoteShardDirectory<TcpAddress, String>>::strong_replace(
                    c, old, new, membership, view,
                )
                .await
            }
        }
    }

    async fn strong_publish_route_changes(
        &self,
        changes: Vec<ShardDirectoryChange<String, TcpAddress>>,
    ) -> Result<(), DiscoveryError> {
        match self {
            Self::Json(c) => {
                <JsonRemoteShardDirectory<TcpAddress> as RemoteShardDirectory<TcpAddress, String>>::strong_publish_route_changes(c, changes).await
            }
            Self::Tapir(c) => {
                <DiscoveryTapirDir as RemoteShardDirectory<TcpAddress, String>>::strong_publish_route_changes(c, changes).await
            }
        }
    }

    async fn weak_route_changes_since(
        &self,
        after_index: u64,
    ) -> Result<Vec<(u64, ShardDirectoryChangeSet<String, TcpAddress>)>, DiscoveryError> {
        match self {
            Self::Json(c) => {
                <JsonRemoteShardDirectory<TcpAddress> as RemoteShardDirectory<TcpAddress, String>>::weak_route_changes_since(c, after_index).await
            }
            Self::Tapir(c) => {
                <DiscoveryTapirDir as RemoteShardDirectory<TcpAddress, String>>::weak_route_changes_since(c, after_index).await
            }
        }
    }
}
