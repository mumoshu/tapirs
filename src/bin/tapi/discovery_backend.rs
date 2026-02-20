use crate::discovery::HttpDiscoveryClient;
use tapirs::discovery::json::JsonRemoteShardDirectory;
use tapirs::discovery::{DiscoveryError, RemoteShardDirectory};
use tapirs::{IrMembership, ShardNumber, TcpAddress};

/// Dispatch enum for [`RemoteShardDirectory<TcpAddress>`] backends.
///
/// `RemoteShardDirectory` uses RPITIT (not object-safe), so a trait object
/// cannot be used. This enum wraps the concrete backend types and delegates
/// each method to the active variant.
pub enum DiscoveryBackend {
    Http(HttpDiscoveryClient),
    Json(JsonRemoteShardDirectory<TcpAddress>),
}

impl RemoteShardDirectory<TcpAddress> for DiscoveryBackend {
    async fn get(
        &self,
        shard: ShardNumber,
    ) -> Result<Option<(IrMembership<TcpAddress>, u64)>, DiscoveryError> {
        match self {
            Self::Http(c) => c.get(shard).await,
            Self::Json(c) => c.get(shard).await,
        }
    }

    async fn put(
        &self,
        shard: ShardNumber,
        membership: IrMembership<TcpAddress>,
        view: u64,
    ) -> Result<(), DiscoveryError> {
        match self {
            Self::Http(c) => c.put(shard, membership, view).await,
            Self::Json(c) => c.put(shard, membership, view).await,
        }
    }

    async fn remove(&self, shard: ShardNumber) -> Result<(), DiscoveryError> {
        match self {
            Self::Http(c) => c.remove(shard).await,
            Self::Json(c) => c.remove(shard).await,
        }
    }

    async fn all(
        &self,
    ) -> Result<Vec<(ShardNumber, IrMembership<TcpAddress>, u64)>, DiscoveryError> {
        match self {
            Self::Http(c) => c.all().await,
            Self::Json(c) => c.all().await,
        }
    }

    async fn replace(
        &self,
        old: ShardNumber,
        new: ShardNumber,
        membership: IrMembership<TcpAddress>,
        view: u64,
    ) -> Result<(), DiscoveryError> {
        match self {
            Self::Http(c) => c.replace(old, new, membership, view).await,
            Self::Json(c) => c.replace(old, new, membership, view).await,
        }
    }
}
