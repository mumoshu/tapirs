use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::{IrMembership, ShardNumber};
use super::{DiscoveryError, RemoteShardDirectory};

/// Read-only [`RemoteShardDirectory`] backed by static membership data or
/// DNS-resolved addresses.
///
/// Intended for use as `CachingShardDirectory`'s remote backend when shard
/// topology comes from a JSON config file (with optional DNS headless service
/// names). Write methods (`put`, `remove`, `replace`) are no-ops — the
/// authoritative source is the config, not runtime state.
///
/// For DNS mode, a background task periodically re-resolves hostnames and
/// updates the in-memory state so `get()`/`all()` return current addresses.
pub struct JsonRemoteShardDirectory<A> {
    state: Arc<RwLock<HashMap<ShardNumber, IrMembership<A>>>>,
    _dns_tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl<A: Clone + Send + Sync + 'static> JsonRemoteShardDirectory<A> {
    /// Create from pre-parsed static membership entries.
    pub fn new(shards: Vec<(ShardNumber, IrMembership<A>)>) -> Self {
        let mut map = HashMap::new();
        for (shard, membership) in shards {
            map.insert(shard, membership);
        }
        Self {
            state: Arc::new(RwLock::new(map)),
            _dns_tasks: Vec::new(),
        }
    }

    /// Add static membership entries to an existing directory (e.g. after
    /// constructing with `with_dns` for DNS shards).
    pub fn add_static_shards(&mut self, shards: Vec<(ShardNumber, IrMembership<A>)>) {
        let mut state = self.state.write().unwrap();
        for (shard, membership) in shards {
            state.insert(shard, membership);
        }
    }
}

impl<A> JsonRemoteShardDirectory<A>
where
    A: From<SocketAddr> + Ord + Copy + Eq + Send + Sync + 'static,
{
    /// Create with DNS-resolved membership that refreshes periodically.
    ///
    /// Each entry in `dns_shards` is `(shard_number, hostname, port)`. Performs
    /// initial DNS resolution for all shards, then spawns background tasks that
    /// re-resolve every `resolve_interval` and update the in-memory state when
    /// IPs change.
    pub async fn with_dns(
        dns_shards: Vec<(ShardNumber, String, u16)>,
        resolve_interval: Duration,
    ) -> Result<Self, String> {
        let state = Arc::new(RwLock::new(HashMap::new()));
        let mut dns_tasks = Vec::new();

        for (shard, host, port) in dns_shards {
            // Initial resolution.
            let addrs = resolve_sorted(&host, port)
                .await
                .map_err(|e| format!("DNS resolution for '{host}:{port}': {e}"))?;
            let membership = IrMembership::new(addrs.iter().copied().map(A::from).collect());
            state.write().unwrap().insert(shard, membership);

            // Background refresh task.
            let state_clone = Arc::clone(&state);
            let task = tokio::spawn(async move {
                let mut current_addrs = addrs;
                loop {
                    tokio::time::sleep(resolve_interval).await;
                    let new_addrs = match resolve_sorted(&host, port).await {
                        Ok(a) => a,
                        Err(_) => continue,
                    };
                    if new_addrs != current_addrs {
                        let membership =
                            IrMembership::new(new_addrs.iter().copied().map(A::from).collect());
                        state_clone.write().unwrap().insert(shard, membership);
                        current_addrs = new_addrs;
                    }
                }
            });
            dns_tasks.push(task);
        }

        Ok(Self {
            state,
            _dns_tasks: dns_tasks,
        })
    }
}

impl<A: Clone + Send + Sync + 'static, K: Clone + Send + Sync + 'static> RemoteShardDirectory<A, K> for JsonRemoteShardDirectory<A> {
    async fn get(
        &self,
        shard: ShardNumber,
    ) -> Result<Option<(IrMembership<A>, u64)>, DiscoveryError> {
        let state = self.state.read().unwrap();
        Ok(state.get(&shard).map(|m| (m.clone(), 0)))
    }

    /// No-op — JSON config is the authoritative source.
    async fn put(
        &self,
        _shard: ShardNumber,
        _membership: IrMembership<A>,
        _view: u64,
    ) -> Result<(), DiscoveryError> {
        Ok(())
    }

    /// No-op — JSON config is the authoritative source.
    async fn remove(&self, _shard: ShardNumber) -> Result<(), DiscoveryError> {
        Ok(())
    }

    #[allow(clippy::disallowed_methods)]
    async fn all(
        &self,
    ) -> Result<Vec<(ShardNumber, IrMembership<A>, u64)>, DiscoveryError> {
        let state = self.state.read().unwrap();
        let mut entries: Vec<_> = state
            .iter()
            .map(|(s, m)| (*s, m.clone(), 0u64))
            .collect();
        entries.sort_by_key(|(s, _, _)| *s);
        Ok(entries)
    }

    /// No-op — JSON config is the authoritative source.
    async fn replace(
        &self,
        _old: ShardNumber,
        _new: ShardNumber,
        _membership: IrMembership<A>,
        _view: u64,
    ) -> Result<(), DiscoveryError> {
        Ok(())
    }
}

/// Resolve a hostname:port to sorted SocketAddrs.
async fn resolve_sorted(host: &str, port: u16) -> Result<Vec<SocketAddr>, std::io::Error> {
    let lookup = format!("{host}:{port}");
    let mut addrs: Vec<SocketAddr> = tokio::net::lookup_host(&lookup).await?.collect();
    if addrs.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "DNS resolution returned zero addresses",
        ));
    }
    addrs.sort();
    Ok(addrs)
}
