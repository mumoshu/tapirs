use super::{Key, ShardClient, ShardNumber, Value};
use crate::{IrClientId, IrMembership, TapirTransport};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// A [`ShardClient`] whose membership is periodically refreshed by resolving
/// a DNS hostname. When resolved IPs change, the internal membership is
/// updated in-place via [`IrClient::reset_membership`].
///
/// The `ShardClient` is accessed via [`DnsRefreshingShardClient::get`] which
/// clones the current client (cheap — `ShardClient` wraps `Arc`). The
/// background DNS task acquires a read lock only when IPs actually change.
pub struct DnsRefreshingShardClient<K: Key, V: Value, T: TapirTransport<K, V>> {
    inner: Arc<RwLock<ShardClient<K, V, T>>>,
    _task: tokio::task::JoinHandle<()>,
}

/// Errors from DNS resolution.
#[derive(Debug)]
pub enum DnsResolveError {
    /// DNS resolution returned zero addresses.
    NoAddresses,
    /// DNS resolution failed.
    ResolveFailed(std::io::Error),
}

impl std::fmt::Display for DnsResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DnsResolveError::NoAddresses => write!(f, "DNS resolution returned zero addresses"),
            DnsResolveError::ResolveFailed(e) => write!(f, "DNS resolution failed: {e}"),
        }
    }
}

impl std::error::Error for DnsResolveError {}

impl<K, V, T> DnsRefreshingShardClient<K, V, T>
where
    K: Key + Clone,
    V: Value + Clone,
    T: TapirTransport<K, V> + Clone,
    T::Address: From<SocketAddr> + Ord,
{
    /// Create a new DNS-refreshing ShardClient.
    ///
    /// Performs an initial DNS resolution, builds the first `ShardClient`,
    /// then spawns a background task that re-resolves every
    /// `resolve_interval`. When IPs change, a new `ShardClient` is built
    /// and swapped atomically via write lock.
    pub async fn new(
        dns_host: String,
        port: u16,
        shard: ShardNumber,
        resolve_interval: Duration,
        transport: T,
        mut rng: crate::Rng,
    ) -> Result<Self, DnsResolveError> {
        let addrs = resolve_sorted(&dns_host, port).await?;
        let membership = IrMembership::new(addrs.iter().copied().map(T::Address::from).collect());
        let client_id = IrClientId::new(&mut rng);
        let client = ShardClient::new(
            rng.fork(),
            client_id,
            shard,
            membership,
            transport.clone(),
        );

        let inner = Arc::new(RwLock::new(client));
        let inner_clone = Arc::clone(&inner);

        let task = tokio::spawn(async move {
            let mut current_addrs = addrs;
            loop {
                tokio::time::sleep(resolve_interval).await;

                let new_addrs = match resolve_sorted(&dns_host, port).await {
                    Ok(a) => a,
                    Err(_) => continue, // transient failure, retry next interval
                };

                if new_addrs != current_addrs {
                    let membership =
                        IrMembership::new(new_addrs.iter().copied().map(T::Address::from).collect());
                    // Update membership in-place rather than creating a new ShardClient.
                    // Creating a new ShardClient would reset the IrClient's operation
                    // counter to 0, causing op_id collisions with entries already in
                    // the replica's record from prior proposals.
                    inner_clone.read().unwrap().inner.reset_membership(membership);
                    current_addrs = new_addrs;
                }
            }
        });

        Ok(Self {
            inner,
            _task: task,
        })
    }

}

impl<K, V, T> DnsRefreshingShardClient<K, V, T>
where
    K: Key,
    V: Value,
    T: TapirTransport<K, V>,
{
    /// Get a clone of the current ShardClient.
    ///
    /// Cloning is cheap (`ShardClient` wraps `Arc<IrClient>`). Callers
    /// use the returned client for operations without holding the lock
    /// across await points.
    pub fn get(&self) -> ShardClient<K, V, T> {
        self.inner.read().unwrap().clone()
    }
}

/// Resolve a hostname:port to sorted SocketAddrs.
async fn resolve_sorted(host: &str, port: u16) -> Result<Vec<SocketAddr>, DnsResolveError> {
    let lookup = format!("{host}:{port}");
    let mut addrs: Vec<SocketAddr> = tokio::net::lookup_host(&lookup)
        .await
        .map_err(DnsResolveError::ResolveFailed)?
        .collect();

    if addrs.is_empty() {
        return Err(DnsResolveError::NoAddresses);
    }

    addrs.sort();
    Ok(addrs)
}
