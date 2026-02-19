use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::{IrMembership, ShardNumber};

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

// ---- RemoteShardDirectory trait ----

/// Async interface to a remote shard directory service.
///
/// Mirrors [`ShardDirectory`] with the same method names (`put`, `remove`,
/// `all`, `replace`) but with async operations and error handling, for
/// network-based directory backends (HTTP discovery, etc.).
///
/// Uses RPITIT (`impl Future`) — no dynamic dispatch. All consumers are generic
/// over `T: RemoteShardDirectory<A>`.
pub trait RemoteShardDirectory<A: Clone + Send + Sync + 'static>: Send + Sync + 'static {
    fn put(
        &self,
        shard: ShardNumber,
        membership: IrMembership<A>,
    ) -> impl std::future::Future<Output = Result<(), DiscoveryError>> + Send + '_;

    fn remove(
        &self,
        shard: ShardNumber,
    ) -> impl std::future::Future<Output = Result<(), DiscoveryError>> + Send + '_;

    fn all(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<(ShardNumber, IrMembership<A>)>, DiscoveryError>>
           + Send
           + '_;

    /// Atomically replace one shard with another.
    ///
    /// Default: non-atomic `remove` + `put`. Implementations should override
    /// for true atomicity.
    fn replace(
        &self,
        old: ShardNumber,
        new: ShardNumber,
        membership: IrMembership<A>,
    ) -> impl std::future::Future<Output = Result<(), DiscoveryError>> + Send + '_ {
        async move {
            let _ = self.remove(old).await;
            self.put(new, membership).await
        }
    }
}

// ---- InMemoryRemoteDirectory ----

/// In-process remote directory for tests.
#[derive(Clone)]
pub struct InMemoryRemoteDirectory<A> {
    state: Arc<RwLock<HashMap<ShardNumber, IrMembership<A>>>>,
}

impl<A> Default for InMemoryRemoteDirectory<A> {
    fn default() -> Self {
        Self {
            state: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl<A> InMemoryRemoteDirectory<A> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<A: Clone> InMemoryRemoteDirectory<A> {
    /// Direct read for test assertions.
    pub fn get(&self, shard: ShardNumber) -> Option<IrMembership<A>> {
        self.state.read().unwrap().get(&shard).cloned()
    }
}

impl<A: Clone + Send + Sync + 'static> RemoteShardDirectory<A> for InMemoryRemoteDirectory<A> {
    async fn put(
        &self,
        shard: ShardNumber,
        membership: IrMembership<A>,
    ) -> Result<(), DiscoveryError> {
        self.state.write().unwrap().insert(shard, membership);
        Ok(())
    }

    async fn remove(&self, shard: ShardNumber) -> Result<(), DiscoveryError> {
        match self.state.write().unwrap().remove(&shard) {
            Some(_) => Ok(()),
            None => Err(DiscoveryError::NotFound),
        }
    }

    #[allow(clippy::disallowed_methods)] // values() iteration order is irrelevant — sorted immediately after
    async fn all(
        &self,
    ) -> Result<Vec<(ShardNumber, IrMembership<A>)>, DiscoveryError> {
        let state = self.state.read().unwrap();
        let mut entries: Vec<_> = state.iter().map(|(k, v)| (*k, v.clone())).collect();
        entries.sort_by_key(|(s, _)| *s);
        Ok(entries)
    }

    async fn replace(
        &self,
        old: ShardNumber,
        new: ShardNumber,
        membership: IrMembership<A>,
    ) -> Result<(), DiscoveryError> {
        let mut state = self.state.write().unwrap();
        state.remove(&old);
        state.insert(new, membership);
        Ok(())
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
/// - [`CachingShardDirectory`]: Wraps `InMemoryShardDirectory` + spawns a
///   background sync task that periodically pushes local state to a
///   [`RemoteShardDirectory`] and pulls remote state (learns about other
///   shards, retries on failure).
pub trait ShardDirectory<A: Clone + Send + Sync + 'static>: Send + Sync + 'static {
    fn get(&self, shard: ShardNumber) -> Option<IrMembership<A>>;
    fn put(&self, shard: ShardNumber, membership: IrMembership<A>);
    fn remove(&self, shard: ShardNumber);
    fn all(&self) -> Vec<(ShardNumber, IrMembership<A>)>;

    /// Atomically replace one shard with another.
    ///
    /// Default: non-atomic `remove` + `put`. Implementations should override
    /// for true atomicity (e.g., single write lock).
    fn replace(&self, old: ShardNumber, new: ShardNumber, membership: IrMembership<A>) {
        self.remove(old);
        self.put(new, membership);
    }
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
    fn replace(&self, old: ShardNumber, new: ShardNumber, membership: IrMembership<A>) {
        (**self).replace(old, new, membership)
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

    fn replace(&self, old: ShardNumber, new: ShardNumber, membership: IrMembership<A>) {
        let mut shards = self.shards.write().unwrap();
        shards.remove(&old);
        shards.insert(new, membership);
    }
}

// ---- CachingShardDirectory ----

/// Caching shard directory that periodically syncs with a remote
/// [`RemoteShardDirectory`].
///
/// Wraps [`InMemoryShardDirectory`] for local reads/writes. A background tokio
/// task periodically:
/// - **Pulls** the full directory from the remote, updating local state
///   for non-own shards (learns about other shards on other nodes).
/// - **Pushes** the own shard's current membership to the remote (retries on
///   next cycle if the push fails).
///
/// All [`ShardDirectory`] methods operate on the local `InMemoryShardDirectory`
/// only — they never block or fail due to remote unavailability.
pub struct CachingShardDirectory<A, T> {
    local: Arc<InMemoryShardDirectory<A>>,
    #[allow(dead_code)]
    remote: Arc<T>,
    own_shards: RwLock<Vec<ShardNumber>>,
    pending_replacements: RwLock<Vec<(ShardNumber, ShardNumber)>>,
}

impl<A, T> CachingShardDirectory<A, T>
where
    A: Clone + Send + Sync + 'static,
    T: RemoteShardDirectory<A>,
{
    /// Create a new caching shard directory and spawn the background sync task.
    ///
    /// The `local` backing store is shared with the transport — the transport
    /// reads/writes it directly, and this directory's background sync task
    /// pushes/pulls changes to/from the remote directory.
    ///
    /// The sync task runs every `sync_interval`, pulling remote entries and
    /// pushing the own shard's membership. It exits automatically when the
    /// returned `Arc` (and all clones) are dropped.
    pub fn new(
        local: Arc<InMemoryShardDirectory<A>>,
        remote: Arc<T>,
        sync_interval: Duration,
    ) -> Arc<Self> {
        let dir = Arc::new(Self {
            local,
            remote,
            own_shards: RwLock::new(Vec::new()),
            pending_replacements: RwLock::new(Vec::new()),
        });

        let weak = Arc::downgrade(&dir);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(sync_interval).await;
                let Some(dir) = weak.upgrade() else {
                    break;
                };

                // Clone the own_shards list once (used by PULL filter and PUSH).
                let own_shards: Vec<ShardNumber> = dir.own_shards.read().unwrap().clone();

                // PULL: fetch all entries, update local for non-own shards only.
                // Own shards are authoritative locally — don't overwrite with
                // potentially stale remote state.
                if let Ok(entries) = dir.remote.all().await {
                    for (shard, membership) in entries {
                        if !own_shards.contains(&shard) {
                            dir.local.put(shard, membership);
                        }
                    }
                }

                // PUSH: re-register own shards' current membership
                // (retries on next cycle if failed).
                for shard in &own_shards {
                    if let Some(membership) = dir.local.get(*shard) {
                        let _ = dir.remote.put(*shard, membership).await;
                    }
                }

                // REPLACE: drain pending replacements and push to remote
                // atomically. Re-queue on failure for next cycle.
                let replacements: Vec<(ShardNumber, ShardNumber)> =
                    std::mem::take(&mut *dir.pending_replacements.write().unwrap());
                let mut failed = Vec::new();
                for (old, new) in replacements {
                    if let Some(membership) = dir.local.get(new) {
                        if dir.remote.replace(old, new, membership).await.is_err() {
                            failed.push((old, new));
                        }
                    }
                }
                if !failed.is_empty() {
                    dir.pending_replacements.write().unwrap().extend(failed);
                }
            }
        });

        dir
    }

    /// Register a shard to push to the remote during background sync.
    /// Typically the shard(s) that the owning node's replicas belong to.
    pub fn add_own_shard(&self, shard: ShardNumber) {
        let mut shards = self.own_shards.write().unwrap();
        if !shards.contains(&shard) {
            shards.push(shard);
        }
    }

    /// Unregister a shard from being pushed to the remote.
    pub fn remove_own_shard(&self, shard: ShardNumber) {
        self.own_shards.write().unwrap().retain(|s| *s != shard);
    }
}

impl<A, T> ShardDirectory<A> for CachingShardDirectory<A, T>
where
    A: Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
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

    fn replace(&self, old: ShardNumber, new: ShardNumber, membership: IrMembership<A>) {
        // Atomic local swap.
        self.local.replace(old, new, membership);
        // Update own_shards tracking: remove old, add new.
        {
            let mut shards = self.own_shards.write().unwrap();
            shards.retain(|s| *s != old);
            if !shards.contains(&new) {
                shards.push(new);
            }
        }
        // Queue for background sync to push atomically to remote.
        self.pending_replacements.write().unwrap().push((old, new));
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

    #[test]
    fn in_memory_directory_replace() {
        let dir = InMemoryShardDirectory::<usize>::new();
        dir.put(ShardNumber(1), IrMembership::new(vec![10, 20, 30]));
        dir.put(ShardNumber(2), IrMembership::new(vec![40, 50, 60]));

        // Replace shard 1 with shard 3.
        dir.replace(ShardNumber(1), ShardNumber(3), IrMembership::new(vec![70, 80]));

        // Old shard gone, new shard present, unrelated shard untouched.
        assert!(dir.get(ShardNumber(1)).is_none());
        let got = dir.get(ShardNumber(3)).unwrap();
        assert_eq!(got.len(), 2);
        assert!(got.contains(70));
        assert!(dir.get(ShardNumber(2)).is_some());
    }

    // ---- InMemoryRemoteDirectory tests ----

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn in_memory_remote_put_and_get() {
        let remote = InMemoryRemoteDirectory::<usize>::new();
        remote
            .put(ShardNumber(1), IrMembership::new(vec![10, 20]))
            .await
            .unwrap();

        let got = remote.get(ShardNumber(1)).unwrap();
        assert_eq!(got.len(), 2);
        assert!(got.contains(10));
        assert!(got.contains(20));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn in_memory_remote_remove() {
        let remote = InMemoryRemoteDirectory::<usize>::new();
        remote
            .put(ShardNumber(1), IrMembership::new(vec![10]))
            .await
            .unwrap();
        remote.remove(ShardNumber(1)).await.unwrap();
        assert!(remote.get(ShardNumber(1)).is_none());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn in_memory_remote_remove_nonexistent() {
        let remote = InMemoryRemoteDirectory::<usize>::new();
        assert!(remote.remove(ShardNumber(99)).await.is_err());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn in_memory_remote_all_sorted() {
        let remote = InMemoryRemoteDirectory::<usize>::new();
        remote
            .put(ShardNumber(3), IrMembership::new(vec![30]))
            .await
            .unwrap();
        remote
            .put(ShardNumber(1), IrMembership::new(vec![10]))
            .await
            .unwrap();

        let entries = remote.all().await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, ShardNumber(1));
        assert_eq!(entries[1].0, ShardNumber(3));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn in_memory_remote_replace() {
        let remote = InMemoryRemoteDirectory::<usize>::new();
        remote
            .put(ShardNumber(1), IrMembership::new(vec![10, 20]))
            .await
            .unwrap();
        remote
            .put(ShardNumber(2), IrMembership::new(vec![30]))
            .await
            .unwrap();

        // Replace shard 1 with shard 3.
        remote
            .replace(
                ShardNumber(1),
                ShardNumber(3),
                IrMembership::new(vec![40, 50]),
            )
            .await
            .unwrap();

        // Old shard gone, new shard present, unrelated shard untouched.
        assert!(remote.get(ShardNumber(1)).is_none());
        let got = remote.get(ShardNumber(3)).unwrap();
        assert_eq!(got.len(), 2);
        assert!(got.contains(40));
        assert!(remote.get(ShardNumber(2)).is_some());
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

    // ---- CachingShardDirectory tests ----

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn caching_directory_local_operations() {
        let remote = Arc::new(InMemoryRemoteDirectory::<usize>::new());
        let local = Arc::new(InMemoryShardDirectory::new());
        let dir = CachingShardDirectory::new(local, remote, Duration::from_secs(60));

        // Local put/get work immediately.
        let shard = ShardNumber(1);
        let membership = IrMembership::new(vec![10, 20, 30]);
        dir.put(shard, membership);
        let got = dir.get(shard).unwrap();
        assert_eq!(got.len(), 3);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn caching_directory_push_to_remote() {
        let remote = Arc::new(InMemoryRemoteDirectory::<usize>::new());
        let local = Arc::new(InMemoryShardDirectory::new());
        let dir =
            CachingShardDirectory::new(local, Arc::clone(&remote), Duration::from_millis(100));

        let shard = ShardNumber(1);
        dir.put(shard, IrMembership::new(vec![10, 20, 30]));
        dir.add_own_shard(shard);

        // Advance time past one sync cycle.
        tokio::time::sleep(Duration::from_millis(150)).await;
        // Yield to let the background task run.
        tokio::task::yield_now().await;

        let pushed = remote.get(ShardNumber(1)).unwrap();
        assert_eq!(pushed.len(), 3);
        assert!(pushed.contains(10));
        assert!(pushed.contains(20));
        assert!(pushed.contains(30));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn caching_directory_pull_from_remote() {
        let remote = Arc::new(InMemoryRemoteDirectory::<usize>::new());
        // Another node registered shard 2 directly.
        remote
            .put(ShardNumber(2), IrMembership::new(vec![40, 50]))
            .await
            .unwrap();

        let local = Arc::new(InMemoryShardDirectory::new());
        let dir =
            CachingShardDirectory::new(local, Arc::clone(&remote), Duration::from_millis(100));

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

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn caching_directory_replace_and_sync() {
        let remote = Arc::new(InMemoryRemoteDirectory::<usize>::new());
        let local = Arc::new(InMemoryShardDirectory::new());
        let dir =
            CachingShardDirectory::new(local, Arc::clone(&remote), Duration::from_millis(100));

        // Set up shard 1 as an own shard and let it sync.
        dir.put(ShardNumber(1), IrMembership::new(vec![10, 20, 30]));
        dir.add_own_shard(ShardNumber(1));

        tokio::time::sleep(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;
        assert!(remote.get(ShardNumber(1)).is_some());

        // Replace shard 1 with shard 2 via the trait method.
        dir.replace(
            ShardNumber(1),
            ShardNumber(2),
            IrMembership::new(vec![40, 50]),
        );

        // Local state updated atomically.
        assert!(dir.get(ShardNumber(1)).is_none());
        let got = dir.get(ShardNumber(2)).unwrap();
        assert_eq!(got.len(), 2);

        // Wait for background sync to drain pending replacement.
        tokio::time::sleep(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;

        // Remote now has shard 2, not shard 1.
        assert!(remote.get(ShardNumber(1)).is_none());
        let pushed = remote.get(ShardNumber(2)).unwrap();
        assert_eq!(pushed.len(), 2);
        assert!(pushed.contains(40));
        assert!(pushed.contains(50));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn caching_directory_pull_skips_own_shards() {
        let remote = Arc::new(InMemoryRemoteDirectory::<usize>::new());
        let local = Arc::new(InMemoryShardDirectory::new());
        let dir =
            CachingShardDirectory::new(local, Arc::clone(&remote), Duration::from_millis(100));

        // Register shard 1 locally with fresh membership.
        dir.put(ShardNumber(1), IrMembership::new(vec![10, 20]));
        dir.add_own_shard(ShardNumber(1));

        // Simulate stale remote state for the same shard.
        remote
            .put(ShardNumber(1), IrMembership::new(vec![99]))
            .await
            .unwrap();

        // After sync, own shard should NOT be overwritten by remote.
        tokio::time::sleep(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;

        let got = dir.get(ShardNumber(1)).unwrap();
        assert_eq!(got.len(), 2); // Still the local value, not the remote "99".
    }
}
