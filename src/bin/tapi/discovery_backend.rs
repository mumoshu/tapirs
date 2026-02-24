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
    async fn weak_get_active_shard_membership(
        &self,
        shard: ShardNumber,
    ) -> Result<Option<(IrMembership<TcpAddress>, u64)>, DiscoveryError> {
        match self {
            Self::Json(c) => {
                <JsonRemoteShardDirectory<TcpAddress> as RemoteShardDirectory<TcpAddress, String>>::weak_get_active_shard_membership(c, shard)
                    .await
            }
            Self::Tapir(c) => {
                <DiscoveryTapirDir as RemoteShardDirectory<TcpAddress, String>>::weak_get_active_shard_membership(c, shard)
                    .await
            }
        }
    }

    async fn strong_put_active_shard_view_membership(
        &self,
        shard: ShardNumber,
        membership: IrMembership<TcpAddress>,
        view: u64,
    ) -> Result<(), DiscoveryError> {
        match self {
            Self::Json(c) => {
                <JsonRemoteShardDirectory<TcpAddress> as RemoteShardDirectory<TcpAddress, String>>::strong_put_active_shard_view_membership(
                    c, shard, membership, view,
                )
                .await
            }
            Self::Tapir(c) => {
                <DiscoveryTapirDir as RemoteShardDirectory<TcpAddress, String>>::strong_put_active_shard_view_membership(
                    c, shard, membership, view,
                )
                .await
            }
        }
    }

    async fn strong_remove_shard(&self, shard: ShardNumber) -> Result<(), DiscoveryError> {
        match self {
            Self::Json(c) => {
                <JsonRemoteShardDirectory<TcpAddress> as RemoteShardDirectory<TcpAddress, String>>::strong_remove_shard(c, shard)
                    .await
            }
            Self::Tapir(c) => {
                <DiscoveryTapirDir as RemoteShardDirectory<TcpAddress, String>>::strong_remove_shard(c, shard)
                    .await
            }
        }
    }

    async fn weak_all_active_shard_view_memberships(
        &self,
    ) -> Result<Vec<(ShardNumber, IrMembership<TcpAddress>, u64)>, DiscoveryError> {
        match self {
            Self::Json(c) => {
                <JsonRemoteShardDirectory<TcpAddress> as RemoteShardDirectory<TcpAddress, String>>::weak_all_active_shard_view_memberships(c).await
            }
            Self::Tapir(c) => {
                <DiscoveryTapirDir as RemoteShardDirectory<TcpAddress, String>>::weak_all_active_shard_view_memberships(c).await
            }
        }
    }

    async fn strong_atomic_update_shards(
        &self,
        changes: Vec<ShardDirectoryChange<String, TcpAddress>>,
    ) -> Result<(), DiscoveryError> {
        match self {
            Self::Json(c) => {
                <JsonRemoteShardDirectory<TcpAddress> as RemoteShardDirectory<TcpAddress, String>>::strong_atomic_update_shards(c, changes).await
            }
            Self::Tapir(c) => {
                <DiscoveryTapirDir as RemoteShardDirectory<TcpAddress, String>>::strong_atomic_update_shards(c, changes).await
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
