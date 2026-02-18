use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{IrMembership, ShardNumber};

// ---- Shared types ----

/// Membership info for a single shard, serialized as JSON for the discovery HTTP API.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardMembership {
    pub id: u32,
    pub replicas: Vec<String>,
}

/// Full cluster topology returned by the discovery service.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterTopology {
    pub shards: Vec<ShardMembership>,
}

/// Error from a discovery service operation.
#[derive(Debug)]
pub enum DiscoveryError {
    NotFound,
    ConnectionFailed(String),
    InvalidResponse(String),
}

impl std::fmt::Display for DiscoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "not found"),
            Self::ConnectionFailed(msg) => write!(f, "connection failed: {msg}"),
            Self::InvalidResponse(msg) => write!(f, "invalid response: {msg}"),
        }
    }
}

impl std::error::Error for DiscoveryError {}

// ---- DiscoveryClient trait ----

/// Interface to an external discovery service for registering/querying shard memberships.
///
/// Uses RPITIT (`impl Future`) — no dynamic dispatch. All consumers are generic
/// over `C: DiscoveryClient`.
pub trait DiscoveryClient: Send + Sync + 'static {
    fn register_shard(
        &self,
        id: u32,
        replicas: Vec<String>,
    ) -> impl std::future::Future<Output = Result<(), DiscoveryError>> + Send + '_;

    fn deregister_shard(
        &self,
        id: u32,
    ) -> impl std::future::Future<Output = Result<(), DiscoveryError>> + Send + '_;

    fn get_topology(
        &self,
    ) -> impl std::future::Future<Output = Result<ClusterTopology, DiscoveryError>> + Send + '_;
}

// ---- InMemoryDiscovery ----

/// In-process discovery service for tests.
#[derive(Clone, Default)]
pub struct InMemoryDiscovery {
    state: Arc<RwLock<HashMap<u32, ShardMembership>>>,
}

impl InMemoryDiscovery {
    pub fn new() -> Self {
        Self::default()
    }

    /// Direct read for test assertions.
    pub fn get_shard(&self, id: u32) -> Option<ShardMembership> {
        self.state.read().unwrap().get(&id).cloned()
    }

    /// Direct read for test assertions.
    #[allow(clippy::disallowed_methods)] // values() iteration order is irrelevant — sorted immediately after
    pub fn topology(&self) -> ClusterTopology {
        let shards = self.state.read().unwrap();
        let mut entries: Vec<ShardMembership> = shards.values().cloned().collect();
        entries.sort_by_key(|s| s.id);
        ClusterTopology { shards: entries }
    }
}

impl DiscoveryClient for InMemoryDiscovery {
    async fn register_shard(&self, id: u32, replicas: Vec<String>) -> Result<(), DiscoveryError> {
        self.state
            .write()
            .unwrap()
            .insert(id, ShardMembership { id, replicas });
        Ok(())
    }

    async fn deregister_shard(&self, id: u32) -> Result<(), DiscoveryError> {
        match self.state.write().unwrap().remove(&id) {
            Some(_) => Ok(()),
            None => Err(DiscoveryError::NotFound),
        }
    }

    async fn get_topology(&self) -> Result<ClusterTopology, DiscoveryError> {
        Ok(self.topology())
    }
}

// ---- ShardDirectory trait ----

/// Unified shard-to-membership directory for cross-shard address resolution.
///
/// TAPIR view changes are intra-shard — they update membership within a single
/// shard but do not propagate across shards. This creates three gaps that
/// `ShardDirectory` exists to fill:
///
/// 1. **No runtime cross-shard membership propagation**: `shard_addresses(shard)`
///    is called by backup coordinator recovery (`replica.rs`) and lazy shard
///    client creation (`client.rs`). Without a shared directory, each transport
///    only knows its own shard's membership, so backup coordinator recovery to
///    other shards hangs unless the config explicitly listed all shards.
///
/// 2. **No propagation of new shards**: Adding a new shard after startup
///    requires all existing clients and replicas to discover it. Without a
///    directory, no mechanism exists for runtime discovery of new shards.
///
/// 3. **No push of membership changes to discovery**: When a view change
///    updates membership, no component pushes to an external discovery service.
///    The discovery service becomes stale without bidirectional sync.
///
/// Two implementations:
/// - [`InMemoryShardDirectory`]: Pure in-memory HashMap. Drop-in replacement
///   for the previous transport-internal shard storage.
/// - [`DiscoveryShardDirectory`]: Wraps `InMemoryShardDirectory` + spawns a
///   background sync task that periodically pushes local state to an external
///   discovery service and pulls remote state (learns about other shards,
///   retries on failure).
pub trait ShardDirectory<A: Clone + Send + Sync + 'static>: Send + Sync + 'static {
    fn get(&self, shard: ShardNumber) -> Option<IrMembership<A>>;
    fn put(&self, shard: ShardNumber, membership: IrMembership<A>);
    fn remove(&self, shard: ShardNumber);
    fn all(&self) -> Vec<(ShardNumber, IrMembership<A>)>;
}

impl<A: Clone + Send + Sync + 'static, T: ShardDirectory<A>> ShardDirectory<A> for Arc<T> {
    fn get(&self, shard: ShardNumber) -> Option<IrMembership<A>> {
        (**self).get(shard)
    }
    fn put(&self, shard: ShardNumber, membership: IrMembership<A>) {
        (**self).put(shard, membership)
    }
    fn remove(&self, shard: ShardNumber) {
        (**self).remove(shard)
    }
    fn all(&self) -> Vec<(ShardNumber, IrMembership<A>)> {
        (**self).all()
    }
}

// ---- InMemoryShardDirectory ----

/// Pure in-memory shard directory backed by a `HashMap` behind `RwLock`.
///
/// Drop-in replacement for the previous `Inner::shards` in channel transport
/// and `shard_directory` in TCP transport.
pub struct InMemoryShardDirectory<A> {
    shards: RwLock<HashMap<ShardNumber, IrMembership<A>>>,
}

impl<A> Default for InMemoryShardDirectory<A> {
    fn default() -> Self {
        Self {
            shards: RwLock::new(HashMap::new()),
        }
    }
}

impl<A> InMemoryShardDirectory<A> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<A: Clone + Send + Sync + 'static> ShardDirectory<A> for InMemoryShardDirectory<A> {
    fn get(&self, shard: ShardNumber) -> Option<IrMembership<A>> {
        self.shards.read().unwrap().get(&shard).cloned()
    }

    fn put(&self, shard: ShardNumber, membership: IrMembership<A>) {
        self.shards.write().unwrap().insert(shard, membership);
    }

    fn remove(&self, shard: ShardNumber) {
        self.shards.write().unwrap().remove(&shard);
    }

    #[allow(clippy::disallowed_methods)] // output order is unspecified; callers must not depend on it
    fn all(&self) -> Vec<(ShardNumber, IrMembership<A>)> {
        self.shards
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }
}

// ---- DiscoveryShardDirectory ----

/// Discovery-backed shard directory that periodically syncs with an external
/// discovery service.
///
/// Wraps [`InMemoryShardDirectory`] for local reads/writes. A background tokio
/// task periodically:
/// - **Pulls** the full cluster topology from discovery, updating local state
///   for all shards (learns about other shards on other nodes).
/// - **Pushes** the own shard's current membership to discovery (retries on
///   next cycle if the push fails).
///
/// All [`ShardDirectory`] methods operate on the local `InMemoryShardDirectory`
/// only — they never block or fail due to discovery unavailability.
pub struct DiscoveryShardDirectory<A, C> {
    local: Arc<InMemoryShardDirectory<A>>,
    discovery: Arc<C>,
    own_shards: RwLock<Vec<ShardNumber>>,
}

impl<A, C> DiscoveryShardDirectory<A, C>
where
    A: Copy + Eq + std::hash::Hash + Display + Send + Sync + 'static,
    A::Err: Display,
    C: DiscoveryClient,
    A: FromStr,
{
    /// Create a new discovery-backed shard directory and spawn the background
    /// sync task.
    ///
    /// The `local` backing store is shared with the transport — the transport
    /// reads/writes it directly, and this directory's background sync task
    /// pushes/pulls changes to/from the external discovery service.
    ///
    /// The sync task runs every `sync_interval`, pulling remote topology and
    /// pushing the own shard's membership. It exits automatically when the
    /// returned `Arc` (and all clones) are dropped.
    pub fn new(
        local: Arc<InMemoryShardDirectory<A>>,
        discovery: Arc<C>,
        sync_interval: Duration,
    ) -> Arc<Self> {
        let dir = Arc::new(Self {
            local,
            discovery,
            own_shards: RwLock::new(Vec::new()),
        });

        let weak = Arc::downgrade(&dir);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(sync_interval).await;
                let Some(dir) = weak.upgrade() else {
                    break;
                };

                // PULL: fetch topology, update local for all shards.
                if let Ok(topology) = dir.discovery.get_topology().await {
                    for shard in topology.shards {
                        if let Ok(membership) = strings_to_membership::<A>(&shard.replicas) {
                            dir.local.put(ShardNumber(shard.id), membership);
                        }
                    }
                }

                // PUSH: re-register own shards' current membership
                // (retries on next cycle if failed).
                // Clone the list and drop the RwLockReadGuard before awaiting.
                let own_shards: Vec<ShardNumber> = dir.own_shards.read().unwrap().clone();
                for shard in own_shards {
                    if let Some(membership) = dir.local.get(shard) {
                        let _ = dir
                            .discovery
                            .register_shard(shard.0, membership_to_strings(&membership))
                            .await;
                    }
                }
            }
        });

        dir
    }

    /// Register a shard to push to discovery during background sync.
    /// Typically the shard(s) that the owning node's replicas belong to.
    pub fn add_own_shard(&self, shard: ShardNumber) {
        let mut shards = self.own_shards.write().unwrap();
        if !shards.contains(&shard) {
            shards.push(shard);
        }
    }

    /// Unregister a shard from being pushed to discovery.
    pub fn remove_own_shard(&self, shard: ShardNumber) {
        self.own_shards.write().unwrap().retain(|s| *s != shard);
    }
}

impl<A, C> ShardDirectory<A> for DiscoveryShardDirectory<A, C>
where
    A: Clone + Send + Sync + 'static,
    C: Send + Sync + 'static,
{
    fn get(&self, shard: ShardNumber) -> Option<IrMembership<A>> {
        self.local.get(shard)
    }

    fn put(&self, shard: ShardNumber, membership: IrMembership<A>) {
        self.local.put(shard, membership);
    }

    fn remove(&self, shard: ShardNumber) {
        self.local.remove(shard);
    }

    fn all(&self) -> Vec<(ShardNumber, IrMembership<A>)> {
        self.local.all()
    }
}

// ---- Address conversion helpers ----

/// Convert a membership's addresses to their string representations.
pub fn membership_to_strings<A: Copy + Eq + Display>(m: &IrMembership<A>) -> Vec<String> {
    m.iter().map(|a| a.to_string()).collect()
}

/// Parse string addresses into an [`IrMembership`].
///
/// Returns an error if any address fails to parse or if the list is empty.
pub fn strings_to_membership<A>(replicas: &[String]) -> Result<IrMembership<A>, String>
where
    A: FromStr + Copy + Eq,
    A::Err: Display,
{
    if replicas.is_empty() {
        return Err("empty replica list".to_string());
    }
    let members: Vec<A> = replicas
        .iter()
        .map(|s| {
            s.parse::<A>()
                .map_err(|e| format!("failed to parse '{}': {}", s, e))
        })
        .collect::<Result<Vec<A>, String>>()?;
    Ok(IrMembership::new(members))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- InMemoryShardDirectory tests ----

    #[test]
    fn in_memory_directory_put_get() {
        let dir = InMemoryShardDirectory::<usize>::new();
        let shard = ShardNumber(1);
        let membership = IrMembership::new(vec![10, 20, 30]);

        assert!(dir.get(shard).is_none());
        dir.put(shard, membership.clone());
        let got = dir.get(shard).unwrap();
        assert_eq!(got.len(), 3);
    }

    #[test]
    fn in_memory_directory_remove() {
        let dir = InMemoryShardDirectory::<usize>::new();
        let shard = ShardNumber(2);
        dir.put(shard, IrMembership::new(vec![1, 2, 3]));
        assert!(dir.get(shard).is_some());
        dir.remove(shard);
        assert!(dir.get(shard).is_none());
    }

    #[test]
    fn in_memory_directory_all() {
        let dir = InMemoryShardDirectory::<usize>::new();
        dir.put(ShardNumber(1), IrMembership::new(vec![10, 20, 30]));
        dir.put(ShardNumber(3), IrMembership::new(vec![40, 50, 60]));
        let mut entries = dir.all();
        entries.sort_by_key(|(s, _)| *s);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, ShardNumber(1));
        assert_eq!(entries[1].0, ShardNumber(3));
    }

    #[test]
    fn in_memory_directory_put_overwrites() {
        let dir = InMemoryShardDirectory::<usize>::new();
        let shard = ShardNumber(1);
        dir.put(shard, IrMembership::new(vec![1, 2, 3]));
        dir.put(shard, IrMembership::new(vec![4, 5]));
        let got = dir.get(shard).unwrap();
        assert_eq!(got.len(), 2);
        assert!(got.contains(4));
    }

    // ---- InMemoryDiscovery tests ----

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn in_memory_discovery_register_and_query() {
        let disc = InMemoryDiscovery::new();
        disc.register_shard(1, vec!["a".into(), "b".into()])
            .await
            .unwrap();

        let shard = disc.get_shard(1).unwrap();
        assert_eq!(shard.id, 1);
        assert_eq!(shard.replicas, vec!["a", "b"]);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn in_memory_discovery_deregister() {
        let disc = InMemoryDiscovery::new();
        disc.register_shard(1, vec!["a".into()]).await.unwrap();
        disc.deregister_shard(1).await.unwrap();
        assert!(disc.get_shard(1).is_none());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn in_memory_discovery_deregister_nonexistent() {
        let disc = InMemoryDiscovery::new();
        assert!(disc.deregister_shard(99).await.is_err());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn in_memory_discovery_topology_sorted() {
        let disc = InMemoryDiscovery::new();
        disc.register_shard(3, vec!["c".into()]).await.unwrap();
        disc.register_shard(1, vec!["a".into()]).await.unwrap();

        let topo = disc.get_topology().await.unwrap();
        assert_eq!(topo.shards.len(), 2);
        assert_eq!(topo.shards[0].id, 1);
        assert_eq!(topo.shards[1].id, 3);
    }

    // ---- Address conversion tests ----

    #[test]
    fn membership_to_strings_usize() {
        let m = IrMembership::new(vec![10usize, 20, 30]);
        let strings = membership_to_strings(&m);
        assert_eq!(strings, vec!["10", "20", "30"]);
    }

    #[test]
    fn strings_to_membership_usize() {
        let strings = vec!["10".into(), "20".into(), "30".into()];
        let m = strings_to_membership::<usize>(&strings).unwrap();
        assert_eq!(m.len(), 3);
        assert!(m.contains(10));
        assert!(m.contains(20));
        assert!(m.contains(30));
    }

    #[test]
    fn strings_to_membership_roundtrip() {
        let original = IrMembership::new(vec![1usize, 2, 3]);
        let strings = membership_to_strings(&original);
        let roundtripped = strings_to_membership::<usize>(&strings).unwrap();
        assert_eq!(roundtripped.len(), original.len());
        for addr in &original {
            assert!(roundtripped.contains(addr));
        }
    }

    #[test]
    fn strings_to_membership_empty() {
        let result = strings_to_membership::<usize>(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn strings_to_membership_parse_error() {
        let strings = vec!["10".into(), "not_a_number".into()];
        let result = strings_to_membership::<usize>(&strings);
        assert!(result.is_err());
    }

    #[test]
    fn strings_to_membership_tcp_address_roundtrip() {
        use crate::TcpAddress;

        let addrs = vec![
            TcpAddress("127.0.0.1:5001".parse().unwrap()),
            TcpAddress("127.0.0.1:5002".parse().unwrap()),
            TcpAddress("127.0.0.1:5003".parse().unwrap()),
        ];
        let original = IrMembership::new(addrs);
        let strings = membership_to_strings(&original);
        assert_eq!(
            strings,
            vec!["127.0.0.1:5001", "127.0.0.1:5002", "127.0.0.1:5003"]
        );
        let roundtripped = strings_to_membership::<TcpAddress>(&strings).unwrap();
        assert_eq!(roundtripped.len(), original.len());
        for addr in &original {
            assert!(roundtripped.contains(addr));
        }
    }

    // ---- DiscoveryShardDirectory tests ----

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn discovery_directory_local_operations() {
        let disc = Arc::new(InMemoryDiscovery::new());
        let local = Arc::new(InMemoryShardDirectory::new());
        let dir = DiscoveryShardDirectory::<usize, _>::new(local, disc, Duration::from_secs(60));

        // Local put/get work immediately.
        let shard = ShardNumber(1);
        let membership = IrMembership::new(vec![10, 20, 30]);
        dir.put(shard, membership);
        let got = dir.get(shard).unwrap();
        assert_eq!(got.len(), 3);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn discovery_directory_push_to_discovery() {
        let disc = Arc::new(InMemoryDiscovery::new());
        let local = Arc::new(InMemoryShardDirectory::new());
        let dir =
            DiscoveryShardDirectory::<usize, _>::new(local, Arc::clone(&disc), Duration::from_millis(100));

        let shard = ShardNumber(1);
        dir.put(shard, IrMembership::new(vec![10, 20, 30]));
        dir.add_own_shard(shard);

        // Advance time past one sync cycle.
        tokio::time::sleep(Duration::from_millis(150)).await;
        // Yield to let the background task run.
        tokio::task::yield_now().await;

        let pushed = disc.get_shard(1).unwrap();
        assert_eq!(pushed.replicas, vec!["10", "20", "30"]);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn discovery_directory_pull_from_discovery() {
        let disc = Arc::new(InMemoryDiscovery::new());
        // Another node registered shard 2 directly.
        disc.register_shard(2, vec!["40".into(), "50".into()])
            .await
            .unwrap();

        let local = Arc::new(InMemoryShardDirectory::new());
        let dir =
            DiscoveryShardDirectory::<usize, _>::new(local, Arc::clone(&disc), Duration::from_millis(100));

        // Initially local doesn't have shard 2.
        assert!(dir.get(ShardNumber(2)).is_none());

        // After one sync cycle, it should be pulled.
        tokio::time::sleep(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;

        let pulled = dir.get(ShardNumber(2)).unwrap();
        assert_eq!(pulled.len(), 2);
        assert!(pulled.contains(40));
        assert!(pulled.contains(50));
    }
}
