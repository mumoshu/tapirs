pub mod json;
pub mod tapir;

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{IrMembership, KeyRange, ShardNumber};

/// A single route change within a [`ShardDirectoryChangeSet`].
///
/// Published atomically by ShardManager during resharding to ensure consumers
/// never see intermediate states with overlapping key ranges.
///
/// `SetRange` includes the shard's membership and view so that
/// `publish_route_changes` can atomically update both the route changelog
/// and the shard membership entries in a single write.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[serde(bound(
    serialize = "K: Serialize, A: Serialize",
    deserialize = "K: serde::de::DeserializeOwned, A: serde::de::DeserializeOwned"
))]
pub enum ShardDirectoryChange<K, A> {
    SetRange {
        shard: ShardNumber,
        range: KeyRange<K>,
        membership: IrMembership<A>,
        view: u64,
    },
    RemoveRange { shard: ShardNumber },
}

/// An atomic set of route changes published at a single index.
pub type ShardDirectoryChangeSet<K, A> = Vec<ShardDirectoryChange<K, A>>;

/// Error from a discovery service operation.
#[derive(Debug)]
pub enum DiscoveryError {
    NotFound,
    /// Shard was decommissioned (removed or replaced); put rejected.
    Tombstoned,
    ConnectionFailed(String),
    InvalidResponse(String),
}

impl std::fmt::Display for DiscoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "not found"),
            Self::Tombstoned => write!(f, "shard tombstoned"),
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
pub trait RemoteShardDirectory<A: Clone + Send + Sync + 'static, K: Clone + Send + Sync + 'static = ()>: Send + Sync + 'static {
    fn get(
        &self,
        shard: ShardNumber,
    ) -> impl std::future::Future<Output = Result<Option<(IrMembership<A>, u64)>, DiscoveryError>>
           + Send
           + '_;

    fn put(
        &self,
        shard: ShardNumber,
        membership: IrMembership<A>,
        view: u64,
    ) -> impl std::future::Future<Output = Result<(), DiscoveryError>> + Send + '_;

    fn remove(
        &self,
        shard: ShardNumber,
    ) -> impl std::future::Future<Output = Result<(), DiscoveryError>> + Send + '_;

    fn all(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<(ShardNumber, IrMembership<A>, u64)>, DiscoveryError>>
           + Send
           + '_;

    /// Publish an atomic set of route changes (additions and removals).
    ///
    /// Atomically updates both the route changelog (for consumers polling via
    /// [`route_changes_since`]) and the shard membership entries. `SetRange`
    /// puts the shard with its membership/view; `RemoveRange` tombstones it.
    ///
    /// Each changeset is indexed sequentially (1-based).
    ///
    /// Default: no-op. Implementations that support route changelog override this.
    fn publish_route_changes(
        &self,
        _changes: Vec<ShardDirectoryChange<K, A>>,
    ) -> impl std::future::Future<Output = Result<(), DiscoveryError>> + Send + '_ {
        async { Ok(()) }
    }

    /// Read route changesets published after `after_index`.
    ///
    /// Returns `(index, changeset)` pairs in ascending order.
    /// Consumers maintain a high watermark and pass it as `after_index`.
    ///
    /// Default: returns empty (no changelog support).
    fn route_changes_since(
        &self,
        _after_index: u64,
    ) -> impl std::future::Future<Output = Result<Vec<(u64, ShardDirectoryChangeSet<K, A>)>, DiscoveryError>>
           + Send
           + '_ {
        async { Ok(vec![]) }
    }

    /// Atomically replace one shard with another.
    ///
    /// Default: non-atomic `remove` + `put`. Implementations should override
    /// for true atomicity.
    fn replace(
        &self,
        old: ShardNumber,
        new: ShardNumber,
        membership: IrMembership<A>,
        view: u64,
    ) -> impl std::future::Future<Output = Result<(), DiscoveryError>> + Send + '_ {
        async move {
            let _ = self.remove(old).await;
            self.put(new, membership, view).await
        }
    }
}

// ---- InMemoryRemoteDirectory ----

struct RemoteDirectoryState<A, K> {
    shards: HashMap<ShardNumber, (IrMembership<A>, u64)>,
    tombstones: HashSet<ShardNumber>,
    /// Per-shard typed key range, set by `publish_route_changes()` during resharding.
    /// Separate from `shards` because `put()` (replica membership updates)
    /// does not carry key ranges.
    key_ranges: HashMap<ShardNumber, KeyRange<K>>,
    /// Append-only route changelog. Index 0 = changeset at index 1 (1-based).
    route_changelog: Vec<ShardDirectoryChangeSet<K, A>>,
}

/// In-process remote directory for tests.
///
/// Supports tombstones: removed/replaced shards are tombstoned and future
/// `put()` calls for those shards are rejected with [`DiscoveryError::Tombstoned`].
/// `all()` omits tombstoned entries.
#[derive(Clone)]
pub struct InMemoryRemoteDirectory<A, K = ()> {
    state: Arc<RwLock<RemoteDirectoryState<A, K>>>,
}

impl<A, K> Default for InMemoryRemoteDirectory<A, K> {
    fn default() -> Self {
        Self {
            state: Arc::new(RwLock::new(RemoteDirectoryState {
                shards: HashMap::new(),
                tombstones: HashSet::new(),
                key_ranges: HashMap::new(),
                route_changelog: Vec::new(),
            })),
        }
    }
}

impl<A, K> InMemoryRemoteDirectory<A, K> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<A: Clone, K> InMemoryRemoteDirectory<A, K> {
    /// Create with initial shard entries (for test setup).
    pub fn with_shards(entries: Vec<(ShardNumber, IrMembership<A>, u64)>) -> Self {
        let mut map = HashMap::new();
        for (shard, membership, view) in entries {
            map.insert(shard, (membership, view));
        }
        Self {
            state: Arc::new(RwLock::new(RemoteDirectoryState {
                shards: map,
                tombstones: HashSet::new(),
                key_ranges: HashMap::new(),
                route_changelog: Vec::new(),
            })),
        }
    }
}

impl<A: Clone + Send + Sync + 'static, K: Clone + Send + Sync + 'static> RemoteShardDirectory<A, K> for InMemoryRemoteDirectory<A, K> {
    async fn get(
        &self,
        shard: ShardNumber,
    ) -> Result<Option<(IrMembership<A>, u64)>, DiscoveryError> {
        let state = self.state.read().unwrap();
        if state.tombstones.contains(&shard) {
            return Ok(None);
        }
        Ok(state.shards.get(&shard).cloned())
    }

    async fn put(
        &self,
        shard: ShardNumber,
        membership: IrMembership<A>,
        view: u64,
    ) -> Result<(), DiscoveryError> {
        let mut state = self.state.write().unwrap();
        if state.tombstones.contains(&shard) {
            return Err(DiscoveryError::Tombstoned);
        }
        match state.shards.get(&shard) {
            Some((_, current_view)) if *current_view > view => return Ok(()), // reject stale
            _ => { state.shards.insert(shard, (membership, view)); }
        }
        Ok(())
    }

    async fn remove(&self, shard: ShardNumber) -> Result<(), DiscoveryError> {
        let mut state = self.state.write().unwrap();
        if state.tombstones.contains(&shard) {
            return Err(DiscoveryError::NotFound);
        }
        match state.shards.remove(&shard) {
            Some(_) => {
                state.key_ranges.remove(&shard);
                state.tombstones.insert(shard);
                Ok(())
            }
            None => Err(DiscoveryError::NotFound),
        }
    }

    #[allow(clippy::disallowed_methods)] // values() iteration order is irrelevant — sorted immediately after
    async fn all(
        &self,
    ) -> Result<Vec<(ShardNumber, IrMembership<A>, u64)>, DiscoveryError> {
        let state = self.state.read().unwrap();
        let mut entries: Vec<_> = state.shards.iter().map(|(k, (m, v))| (*k, m.clone(), *v)).collect();
        entries.sort_by_key(|(s, _, _)| *s);
        Ok(entries)
    }

    async fn replace(
        &self,
        old: ShardNumber,
        new: ShardNumber,
        membership: IrMembership<A>,
        view: u64,
    ) -> Result<(), DiscoveryError> {
        let mut state = self.state.write().unwrap();
        // Tombstone old shard.
        state.shards.remove(&old);
        state.key_ranges.remove(&old);
        state.tombstones.insert(old);
        // Insert new shard with monotonic check.
        match state.shards.get(&new) {
            Some((_, current_view)) if *current_view > view => {}
            _ => { state.shards.insert(new, (membership, view)); }
        }
        Ok(())
    }

    async fn publish_route_changes(
        &self,
        changes: Vec<ShardDirectoryChange<K, A>>,
    ) -> Result<(), DiscoveryError> {
        let mut state = self.state.write().unwrap();
        // Atomically update key_ranges, shard entries, and changelog.
        for change in &changes {
            match change {
                ShardDirectoryChange::SetRange { shard, range, membership, view } => {
                    state.key_ranges.insert(*shard, range.clone());
                    // Monotonic view check for membership.
                    match state.shards.get(shard) {
                        Some((_, current_view)) if *current_view > *view => {}
                        _ => { state.shards.insert(*shard, (membership.clone(), *view)); }
                    }
                }
                ShardDirectoryChange::RemoveRange { shard } => {
                    state.key_ranges.remove(shard);
                    state.shards.remove(shard);
                    state.tombstones.insert(*shard);
                }
            }
        }
        state.route_changelog.push(changes);
        Ok(())
    }

    async fn route_changes_since(
        &self,
        after_index: u64,
    ) -> Result<Vec<(u64, ShardDirectoryChangeSet<K, A>)>, DiscoveryError> {
        let state = self.state.read().unwrap();
        let start = after_index as usize; // changelog[0] = index 1
        if start >= state.route_changelog.len() {
            return Ok(vec![]);
        }
        Ok(state.route_changelog[start..]
            .iter()
            .enumerate()
            .map(|(i, cs)| ((start + i + 1) as u64, cs.clone()))
            .collect())
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
    fn get(&self, shard: ShardNumber) -> Option<(IrMembership<A>, u64)>;
    fn put(&self, shard: ShardNumber, membership: IrMembership<A>, view: u64);
    fn remove(&self, shard: ShardNumber);
    fn all(&self) -> Vec<(ShardNumber, IrMembership<A>, u64)>;

    /// Atomically replace one shard with another.
    ///
    /// Default: non-atomic `remove` + `put`. Implementations should override
    /// for true atomicity (e.g., single write lock).
    fn replace(&self, old: ShardNumber, new: ShardNumber, membership: IrMembership<A>, view: u64) {
        self.remove(old);
        self.put(new, membership, view);
    }
}

impl<A: Clone + Send + Sync + 'static, T: ShardDirectory<A>> ShardDirectory<A> for Arc<T> {
    fn get(&self, shard: ShardNumber) -> Option<(IrMembership<A>, u64)> {
        (**self).get(shard)
    }
    fn put(&self, shard: ShardNumber, membership: IrMembership<A>, view: u64) {
        (**self).put(shard, membership, view)
    }
    fn remove(&self, shard: ShardNumber) {
        (**self).remove(shard)
    }
    fn all(&self) -> Vec<(ShardNumber, IrMembership<A>, u64)> {
        (**self).all()
    }
    fn replace(&self, old: ShardNumber, new: ShardNumber, membership: IrMembership<A>, view: u64) {
        (**self).replace(old, new, membership, view)
    }
}

// ---- InMemoryShardDirectory ----

/// Pure in-memory shard directory backed by a `HashMap` behind `RwLock`.
///
/// Drop-in replacement for the previous `Inner::shards` in channel transport
/// and `shard_directory` in TCP transport.
pub struct InMemoryShardDirectory<A> {
    shards: RwLock<HashMap<ShardNumber, (IrMembership<A>, u64)>>,
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
    fn get(&self, shard: ShardNumber) -> Option<(IrMembership<A>, u64)> {
        self.shards.read().unwrap().get(&shard).cloned()
    }

    fn put(&self, shard: ShardNumber, membership: IrMembership<A>, view: u64) {
        let mut shards = self.shards.write().unwrap();
        match shards.get(&shard) {
            Some((_, current_view)) if *current_view > view => (), // reject stale
            _ => { shards.insert(shard, (membership, view)); }
        }
    }

    fn remove(&self, shard: ShardNumber) {
        self.shards.write().unwrap().remove(&shard);
    }

    #[allow(clippy::disallowed_methods)] // output order is unspecified; callers must not depend on it
    fn all(&self) -> Vec<(ShardNumber, IrMembership<A>, u64)> {
        self.shards
            .read()
            .unwrap()
            .iter()
            .map(|(k, (m, v))| (*k, m.clone(), *v))
            .collect()
    }

    fn replace(&self, old: ShardNumber, new: ShardNumber, membership: IrMembership<A>, view: u64) {
        let mut shards = self.shards.write().unwrap();
        shards.remove(&old);
        match shards.get(&new) {
            Some((_, current_view)) if *current_view > view => {}
            _ => { shards.insert(new, (membership, view)); }
        }
    }
}

// ---- CachingShardDirectory ----

/// Caching shard directory that periodically syncs with a remote
/// [`RemoteShardDirectory`].
///
/// Wraps [`InMemoryShardDirectory`] for local reads/writes. A background tokio
/// task periodically:
/// - **Pulls** all entries from the remote, writing to local. Monotonic `put()`
///   rejects stale entries (view < current), so alive replica's local data is
///   never overwritten by stale remote data. Tombstoned shards are omitted by
///   `remote.all()`.
/// - **Pushes** only **own_shards** entries from local to remote. Prevents
///   O(N×S) echo traffic where pulled entries are pushed back. Remote rejects
///   stale pushes (view < current) and tombstoned shards — both harmless.
///
/// All [`ShardDirectory`] methods operate on the local `InMemoryShardDirectory`
/// only — they never block or fail due to remote unavailability.
pub struct CachingShardDirectory<A, K, T> {
    local: Arc<InMemoryShardDirectory<A>>,
    #[allow(dead_code)]
    remote: Arc<T>,
    /// Shards hosted by this node's replicas — used as a **PUSH filter only**.
    ///
    /// **Purpose**: Prevents O(N×S) echo traffic. Without this filter, every
    /// node would push ALL local entries (including data originally pulled from
    /// remote) back to remote on every sync cycle. With `own_shards`, only
    /// entries where this node is authoritative (hosts a replica) are pushed.
    ///
    /// **CRITICAL: Never use for PULL filtering.** The original livelock bug
    /// was caused by `own_shards` blocking PULL: when a replica dies, the
    /// node's local membership becomes stale, but PULL was skipped because
    /// the shard was in `own_shards`. This prevented the node from learning
    /// the updated membership from remote, and the stale PUSH corrupted the
    /// remote for other nodes.
    ///
    /// **Correct usage**:
    /// - PULL: write ALL remote entries to local. `put()` is monotonic —
    ///   rejects if remote_view < local_view, so alive replicas' local data
    ///   is never overwritten by stale remote data.
    /// - PUSH: push only entries where shard ∈ `own_shards`.
    ///
    /// Clients have empty `own_shards` — they are PULL-only.
    own_shards: RwLock<HashSet<ShardNumber>>,
    /// Key ranges managed via route changelog.
    /// Updated by applying changesets from `route_changes_since()`.
    key_ranges: RwLock<HashMap<ShardNumber, KeyRange<K>>>,
    /// High watermark for route changelog consumption (1-based index).
    /// Never regresses — stale eventual reads are ignored.
    route_hwm: RwLock<u64>,
}

impl<A, K, T> CachingShardDirectory<A, K, T>
where
    A: Clone + Send + Sync + 'static,
    K: Clone + Send + Sync + 'static,
    T: RemoteShardDirectory<A, K>,
{
    /// Create a new caching shard directory and spawn the background sync task.
    ///
    /// The `local` backing store is shared with the transport — the transport
    /// reads/writes it directly, and this directory's background sync task
    /// pushes/pulls changes to/from the remote directory.
    ///
    /// The sync task runs every `sync_interval`, pulling remote entries and
    /// pushing own_shards entries. It exits automatically when the returned
    /// `Arc` (and all clones) are dropped.
    pub fn new(
        local: Arc<InMemoryShardDirectory<A>>,
        remote: Arc<T>,
        sync_interval: Duration,
    ) -> Arc<Self> {
        let dir = Arc::new(Self {
            local,
            remote,
            own_shards: RwLock::new(HashSet::new()),
            key_ranges: RwLock::new(HashMap::new()),
            route_hwm: RwLock::new(0),
        });

        let weak = Arc::downgrade(&dir);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(sync_interval).await;
                let Some(dir) = weak.upgrade() else {
                    break;
                };

                // PULL membership: write ALL remote entries to local (no own_shards check).
                // put() is monotonic — rejects if remote_view < current local_view.
                // Alive replica's local (higher view) is never overwritten.
                // Dead replica's local (lower view) gets overwritten by fresh remote data.
                // Tombstoned shards are omitted by remote.all().
                if let Ok(entries) = dir.remote.all().await {
                    for (shard, membership, remote_view) in entries {
                        dir.local.put(shard, membership, remote_view);
                    }
                }

                // PULL route changelog: apply atomic changesets to key_ranges.
                // High watermark never regresses — stale eventual reads are ignored.
                // Within each changeset, process RemoveRange before SetRange to
                // avoid transient overlapping ranges when a shard is replaced.
                let hwm = *dir.route_hwm.read().unwrap();
                if let Ok(changesets) = dir.remote.route_changes_since(hwm).await {
                    let mut ranges = dir.key_ranges.write().unwrap();
                    let mut new_hwm = hwm;
                    for (idx, changes) in changesets {
                        if idx != new_hwm + 1 {
                            break; // gap — wait for eventual consistency
                        }
                        // Deletes first, then inserts.
                        for change in &changes {
                            if let ShardDirectoryChange::RemoveRange { shard } = change {
                                ranges.remove(shard);
                            }
                        }
                        for change in changes {
                            if let ShardDirectoryChange::SetRange { shard, range, .. } = change {
                                ranges.insert(shard, range);
                            }
                        }
                        new_hwm = idx;
                    }
                    drop(ranges);
                    *dir.route_hwm.write().unwrap() = new_hwm;
                }

                // PUSH: push only own_shards entries to remote.
                // Prevents echo traffic (pulled entries pushed back).
                // Remote rejects stale pushes (view < current) — harmless.
                // Remote tombstones reject puts for decommissioned shards — harmless.
                let own = dir.own_shards.read().unwrap().clone();
                for (shard, membership, local_view) in dir.local.all() {
                    if own.contains(&shard) {
                        let _ = dir.remote.put(shard, membership, local_view).await;
                    }
                }
            }
        });

        dir
    }

    /// Return the current cached key ranges (pulled from remote).
    pub fn key_ranges(&self) -> HashMap<ShardNumber, KeyRange<K>> {
        self.key_ranges.read().unwrap().clone()
    }

    /// Return the current route changelog high watermark (1-based index).
    ///
    /// Increases monotonically as changesets are consumed.  Returns 0 if
    /// no changesets have been applied yet.
    pub fn route_hwm(&self) -> u64 {
        *self.route_hwm.read().unwrap()
    }

    /// Register a shard as hosted by this node (enables PUSH for it).
    pub fn add_own_shard(&self, shard: ShardNumber) {
        self.own_shards.write().unwrap().insert(shard);
    }

    /// Unregister a shard from this node (disables PUSH for it).
    pub fn remove_own_shard(&self, shard: ShardNumber) {
        self.own_shards.write().unwrap().remove(&shard);
    }
}

impl<A, K, T> ShardDirectory<A> for CachingShardDirectory<A, K, T>
where
    A: Clone + Send + Sync + 'static,
    K: Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
{
    fn get(&self, shard: ShardNumber) -> Option<(IrMembership<A>, u64)> {
        self.local.get(shard)
    }

    fn put(&self, shard: ShardNumber, membership: IrMembership<A>, view: u64) {
        self.local.put(shard, membership, view);
    }

    fn remove(&self, shard: ShardNumber) {
        self.local.remove(shard);
    }

    fn all(&self) -> Vec<(ShardNumber, IrMembership<A>, u64)> {
        self.local.all()
    }

    // replace() uses default impl → self.remove(old) + self.put(new, m, v)
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
        dir.put(shard, membership.clone(), 0);
        let (got, view) = dir.get(shard).unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(view, 0);
    }

    #[test]
    fn in_memory_directory_remove() {
        let dir = InMemoryShardDirectory::<usize>::new();
        let shard = ShardNumber(2);
        dir.put(shard, IrMembership::new(vec![1, 2, 3]), 0);
        assert!(dir.get(shard).is_some());
        dir.remove(shard);
        assert!(dir.get(shard).is_none());
    }

    #[test]
    fn in_memory_directory_all() {
        let dir = InMemoryShardDirectory::<usize>::new();
        dir.put(ShardNumber(1), IrMembership::new(vec![10, 20, 30]), 0);
        dir.put(ShardNumber(3), IrMembership::new(vec![40, 50, 60]), 0);
        let mut entries = dir.all();
        entries.sort_by_key(|(s, _, _)| *s);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, ShardNumber(1));
        assert_eq!(entries[1].0, ShardNumber(3));
    }

    #[test]
    fn in_memory_directory_put_overwrites_with_higher_view() {
        let dir = InMemoryShardDirectory::<usize>::new();
        let shard = ShardNumber(1);
        dir.put(shard, IrMembership::new(vec![1, 2, 3]), 1);
        dir.put(shard, IrMembership::new(vec![4, 5]), 2);
        let (got, view) = dir.get(shard).unwrap();
        assert_eq!(got.len(), 2);
        assert!(got.contains(4));
        assert_eq!(view, 2);
    }

    #[test]
    fn in_memory_directory_put_rejects_stale() {
        let dir = InMemoryShardDirectory::<usize>::new();
        let shard = ShardNumber(1);
        dir.put(shard, IrMembership::new(vec![1, 2, 3]), 5);
        dir.put(shard, IrMembership::new(vec![4, 5]), 3); // stale — rejected
        let (got, view) = dir.get(shard).unwrap();
        assert_eq!(got.len(), 3); // still the original
        assert_eq!(view, 5);
    }

    #[test]
    fn in_memory_directory_replace() {
        let dir = InMemoryShardDirectory::<usize>::new();
        dir.put(ShardNumber(1), IrMembership::new(vec![10, 20, 30]), 0);
        dir.put(ShardNumber(2), IrMembership::new(vec![40, 50, 60]), 0);

        // Replace shard 1 with shard 3.
        dir.replace(ShardNumber(1), ShardNumber(3), IrMembership::new(vec![70, 80]), 0);

        // Old shard gone, new shard present, unrelated shard untouched.
        assert!(dir.get(ShardNumber(1)).is_none());
        let (got, _) = dir.get(ShardNumber(3)).unwrap();
        assert_eq!(got.len(), 2);
        assert!(got.contains(70));
        assert!(dir.get(ShardNumber(2)).is_some());
    }

    // ---- InMemoryRemoteDirectory tests ----

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn in_memory_remote_put_and_get() {
        let remote = InMemoryRemoteDirectory::<usize>::new();
        remote
            .put(ShardNumber(1), IrMembership::new(vec![10, 20]), 0)
            .await
            .unwrap();

        let (got, view) = remote.get(ShardNumber(1)).await.unwrap().unwrap();
        assert_eq!(got.len(), 2);
        assert!(got.contains(10));
        assert!(got.contains(20));
        assert_eq!(view, 0);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn in_memory_remote_remove() {
        let remote = InMemoryRemoteDirectory::<usize>::new();
        remote
            .put(ShardNumber(1), IrMembership::new(vec![10]), 0)
            .await
            .unwrap();
        remote.remove(ShardNumber(1)).await.unwrap();
        assert!(remote.get(ShardNumber(1)).await.unwrap().is_none());
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
            .put(ShardNumber(3), IrMembership::new(vec![30]), 0)
            .await
            .unwrap();
        remote
            .put(ShardNumber(1), IrMembership::new(vec![10]), 0)
            .await
            .unwrap();

        let entries = remote.all().await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, ShardNumber(1));
        assert_eq!(entries[1].0, ShardNumber(3));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn in_memory_remote_put_rejects_stale() {
        let remote = InMemoryRemoteDirectory::<usize>::new();
        remote
            .put(ShardNumber(1), IrMembership::new(vec![10, 20]), 5)
            .await
            .unwrap();
        remote
            .put(ShardNumber(1), IrMembership::new(vec![99]), 3)
            .await
            .unwrap();

        let (got, view) = remote.get(ShardNumber(1)).await.unwrap().unwrap();
        assert_eq!(got.len(), 2); // still the original
        assert_eq!(view, 5);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn in_memory_remote_replace() {
        let remote = InMemoryRemoteDirectory::<usize>::new();
        remote
            .put(ShardNumber(1), IrMembership::new(vec![10, 20]), 0)
            .await
            .unwrap();
        remote
            .put(ShardNumber(2), IrMembership::new(vec![30]), 0)
            .await
            .unwrap();

        // Replace shard 1 with shard 3.
        remote
            .replace(
                ShardNumber(1),
                ShardNumber(3),
                IrMembership::new(vec![40, 50]),
                0,
            )
            .await
            .unwrap();

        // Old shard gone, new shard present, unrelated shard untouched.
        assert!(remote.get(ShardNumber(1)).await.unwrap().is_none());
        let (got, _) = remote.get(ShardNumber(3)).await.unwrap().unwrap();
        assert_eq!(got.len(), 2);
        assert!(got.contains(40));
        assert!(remote.get(ShardNumber(2)).await.unwrap().is_some());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn in_memory_remote_tombstone_rejects_put() {
        let remote = InMemoryRemoteDirectory::<usize>::new();
        remote
            .put(ShardNumber(1), IrMembership::new(vec![10, 20]), 0)
            .await
            .unwrap();

        // Remove creates a tombstone.
        remote.remove(ShardNumber(1)).await.unwrap();
        assert!(remote.get(ShardNumber(1)).await.unwrap().is_none());

        // Put to tombstoned shard is rejected.
        let result = remote
            .put(ShardNumber(1), IrMembership::new(vec![99]), 5)
            .await;
        assert!(matches!(result, Err(DiscoveryError::Tombstoned)));

        // Replace also tombstones the old shard.
        remote
            .put(ShardNumber(2), IrMembership::new(vec![30]), 0)
            .await
            .unwrap();
        remote
            .replace(ShardNumber(2), ShardNumber(3), IrMembership::new(vec![40]), 0)
            .await
            .unwrap();

        // Old shard (2) is tombstoned — put rejected.
        let result = remote
            .put(ShardNumber(2), IrMembership::new(vec![99]), 5)
            .await;
        assert!(matches!(result, Err(DiscoveryError::Tombstoned)));

        // New shard (3) is alive — put succeeds.
        remote
            .put(ShardNumber(3), IrMembership::new(vec![50]), 1)
            .await
            .unwrap();
        let (got, _) = remote.get(ShardNumber(3)).await.unwrap().unwrap();
        assert!(got.contains(50));
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
        dir.put(shard, membership, 0);
        let (got, _) = dir.get(shard).unwrap();
        assert_eq!(got.len(), 3);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn caching_directory_push_to_remote() {
        let remote = Arc::new(InMemoryRemoteDirectory::<usize>::new());
        let local = Arc::new(InMemoryShardDirectory::new());
        let dir =
            CachingShardDirectory::new(local, Arc::clone(&remote), Duration::from_millis(100));

        let shard = ShardNumber(1);
        dir.add_own_shard(shard);
        dir.put(shard, IrMembership::new(vec![10, 20, 30]), 0);

        // Advance time past one sync cycle.
        tokio::time::sleep(Duration::from_millis(150)).await;
        // Yield to let the background task run.
        tokio::task::yield_now().await;

        let (pushed, _) = remote.get(ShardNumber(1)).await.unwrap().unwrap();
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
            .put(ShardNumber(2), IrMembership::new(vec![40, 50]), 0)
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

        let (pulled, _) = dir.get(ShardNumber(2)).unwrap();
        assert_eq!(pulled.len(), 2);
        assert!(pulled.contains(40));
        assert!(pulled.contains(50));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn caching_directory_replace_and_sync() {
        // Test tombstone flow: remote.replace() tombstones old shard,
        // CachingShardDirectory's PULL picks up the new shard, and PUSH
        // of stale old shard is rejected by tombstone.
        let remote = Arc::new(InMemoryRemoteDirectory::<usize>::new());
        let local = Arc::new(InMemoryShardDirectory::new());
        let dir =
            CachingShardDirectory::new(local, Arc::clone(&remote), Duration::from_millis(100));

        // Node hosts shard 1.
        dir.add_own_shard(ShardNumber(1));
        dir.put(ShardNumber(1), IrMembership::new(vec![10, 20, 30]), 0);

        // Let it sync — shard 1 pushed to remote.
        tokio::time::sleep(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;
        assert!(remote.get(ShardNumber(1)).await.unwrap().is_some());

        // Simulate ShardManager's replace: tombstone old shard in remote directly.
        remote
            .replace(
                ShardNumber(1),
                ShardNumber(2),
                IrMembership::new(vec![40, 50]),
                0,
            )
            .await
            .unwrap();

        // Remote: shard 1 tombstoned, shard 2 present.
        assert!(remote.get(ShardNumber(1)).await.unwrap().is_none());
        assert!(remote.get(ShardNumber(2)).await.unwrap().is_some());

        // Next sync cycle: PULL picks up shard 2, PUSH of shard 1 is rejected (tombstoned).
        tokio::time::sleep(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;

        // Local now has shard 2 from PULL.
        let (pulled, _) = dir.get(ShardNumber(2)).unwrap();
        assert_eq!(pulled.len(), 2);
        assert!(pulled.contains(40));

        // Remote shard 1 remains tombstoned — PUSH didn't overwrite.
        assert!(remote.get(ShardNumber(1)).await.unwrap().is_none());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn caching_directory_monotonic_pull_rejects_stale() {
        let remote = Arc::new(InMemoryRemoteDirectory::<usize>::new());
        let local = Arc::new(InMemoryShardDirectory::new());
        let dir =
            CachingShardDirectory::new(local, Arc::clone(&remote), Duration::from_millis(100));

        // Register shard 1 locally at view 5 (simulates alive replica after view change).
        dir.put(ShardNumber(1), IrMembership::new(vec![10, 20]), 5);

        // Simulate stale remote state for the same shard at view 3.
        remote
            .put(ShardNumber(1), IrMembership::new(vec![99]), 3)
            .await
            .unwrap();

        // After sync, PULL writes remote entry but put() rejects it (3 < 5).
        tokio::time::sleep(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;

        let (got, view) = dir.get(ShardNumber(1)).unwrap();
        assert_eq!(got.len(), 2); // Still the local value, not the remote "99".
        assert_eq!(view, 5);
    }
}
