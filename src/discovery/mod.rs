
pub mod backend;
pub mod json;
pub mod tapir;

pub use tapir::ShardStatus;

/// Full shard metadata returned by [`RemoteShardDirectory::strong_get_shard`].
///
/// Unlike the previous `(IrMembership<A>, u64)` tuple, this includes
/// [`ShardStatus`] and [`KeyRange`] so callers get the complete shard
/// record without the directory filtering out non-Active entries.
#[derive(Debug, Clone)]
pub struct ShardRecord<A, K> {
    pub membership: IrMembership<A>,
    pub view: u64,
    pub status: ShardStatus,
    pub key_range: Option<KeyRange<K>>,
}

use std::collections::{BTreeMap, HashSet};
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
/// `ActivateShard` includes the shard's membership and view so that
/// `strong_atomic_update_shards` can atomically update both the route changelog
/// and the shard membership entries in a single write.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[serde(bound(
    serialize = "K: Serialize, A: Serialize",
    deserialize = "K: serde::de::DeserializeOwned, A: serde::de::DeserializeOwned"
))]
pub enum ShardDirectoryChange<K, A> {
    ActivateShard {
        shard: ShardNumber,
        range: KeyRange<K>,
        membership: IrMembership<A>,
        view: u64,
    },
    TombstoneShard { shard: ShardNumber },
}

/// An atomic set of route changes published at a single index.
pub type ShardDirectoryChangeSet<K, A> = Vec<ShardDirectoryChange<K, A>>;

/// Error from a discovery service operation.
#[derive(Debug)]
pub enum DiscoveryError {
    NotFound,
    /// Shard was decommissioned (removed or replaced); put rejected.
    Tombstoned,
    /// Shard is not in Active status (e.g. Pending); put rejected.
    NotActive,
    ConnectionFailed(String),
    InvalidResponse(String),
}

impl std::fmt::Display for DiscoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "not found"),
            Self::Tombstoned => write!(f, "shard tombstoned"),
            Self::NotActive => write!(f, "shard not active"),
            Self::ConnectionFailed(msg) => write!(f, "connection failed: {msg}"),
            Self::InvalidResponse(msg) => write!(f, "invalid response: {msg}"),
        }
    }
}

impl std::error::Error for DiscoveryError {}

// ---- RemoteShardDirectory trait ----

/// Async interface to a remote shard directory service.
///
/// Async counterpart of [`ShardDirectory`] for network-based directory
/// backends (TAPIR discovery cluster, HTTP, etc.).
///
/// Methods prefixed with `strong_` use quorum operations for strong
/// consistency — `strong_get_shard` uses RO transactions (linearizable
/// reads), and write methods use RW transactions. Methods prefixed with
/// `weak_` use eventual consistency (unlogged reads to 1 replica).
///
/// Uses RPITIT (`impl Future`) — no dynamic dispatch. All consumers are generic
/// over `T: RemoteShardDirectory<A>`.
pub trait RemoteShardDirectory<A: Clone + Send + Sync + 'static, K: Clone + Send + Sync + 'static = ()>: Send + Sync + 'static {
    fn strong_get_shard(
        &self,
        shard: ShardNumber,
    ) -> impl std::future::Future<Output = Result<Option<ShardRecord<A, K>>, DiscoveryError>>
           + Send
           + '_;

    fn strong_put_active_shard_view_membership(
        &self,
        shard: ShardNumber,
        membership: IrMembership<A>,
        view: u64,
    ) -> impl std::future::Future<Output = Result<(), DiscoveryError>> + Send + '_;

    fn weak_all_active_shard_view_memberships(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<(ShardNumber, IrMembership<A>, u64)>, DiscoveryError>>
           + Send
           + '_;

    fn strong_all_active_shard_view_memberships(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<(ShardNumber, IrMembership<A>, u64)>, DiscoveryError>>
           + Send
           + '_;

    /// Atomically update shard entries and the route changelog.
    ///
    /// Applies `ActivateShard` (puts the shard with membership/view) and
    /// `TombstoneShard` (tombstones it) changes. Each changeset is indexed
    /// sequentially (1-based) for consumers polling via
    /// [`weak_route_changes_since`].
    fn strong_atomic_update_shards(
        &self,
        changes: Vec<ShardDirectoryChange<K, A>>,
    ) -> impl std::future::Future<Output = Result<(), DiscoveryError>> + Send + '_;

    /// Read route changesets published after `after_index`.
    ///
    /// Returns `(index, changeset)` pairs in ascending order.
    /// Consumers maintain a high watermark and pass it as `after_index`.
    ///
    /// Default: returns empty (no changelog support).
    fn weak_route_changes_since(
        &self,
        _after_index: u64,
    ) -> impl std::future::Future<Output = Result<Vec<(u64, ShardDirectoryChangeSet<K, A>)>, DiscoveryError>>
           + Send
           + '_ {
        async { Ok(vec![]) }
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
/// - [`InMemoryShardDirectory`]: Pure in-memory BTreeMap. Drop-in replacement
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

/// Pure in-memory shard directory backed by a `BTreeMap` behind `RwLock`.
///
/// Drop-in replacement for the previous `Inner::shards` in channel transport
/// and `shard_directory` in TCP transport.
pub struct InMemoryShardDirectory<A> {
    shards: RwLock<BTreeMap<ShardNumber, (IrMembership<A>, u64)>>,
}

impl<A> Default for InMemoryShardDirectory<A> {
    fn default() -> Self {
        Self {
            shards: RwLock::new(BTreeMap::new()),
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
    key_ranges: RwLock<BTreeMap<ShardNumber, KeyRange<K>>>,
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
            key_ranges: RwLock::new(BTreeMap::new()),
            route_hwm: RwLock::new(0),
        });

        let weak = Arc::downgrade(&dir);
        tokio::spawn(async move {
            let mut cycle_count = 0u64;
            loop {
                tokio::time::sleep(sync_interval).await;
                let Some(dir) = weak.upgrade() else {
                    break;
                };
                let cycle_wall_start = std::time::Instant::now();

                // PULL membership: write ALL remote entries to local (no own_shards check).
                // put() is monotonic — rejects if remote_view < current local_view.
                // Alive replica's local (higher view) is never overwritten.
                // Dead replica's local (lower view) gets overwritten by fresh remote data.
                // Tombstoned shards are omitted by remote.all().
                let pull_start = std::time::Instant::now();
                if let Ok(entries) = dir.remote.weak_all_active_shard_view_memberships().await {
                    for (shard, membership, remote_view) in entries {
                        dir.local.put(shard, membership, remote_view);
                    }
                }
                let pull_ms = pull_start.elapsed().as_millis();

                // PULL route changelog: apply atomic changesets to key_ranges.
                // High watermark never regresses — stale eventual reads are ignored.
                // Within each changeset, process TombstoneShard before ActivateShard to
                // avoid transient overlapping ranges when a shard is replaced.
                let route_start = std::time::Instant::now();
                let hwm = *dir.route_hwm.read().unwrap();
                if let Ok(changesets) = dir.remote.weak_route_changes_since(hwm).await {
                    let mut ranges = dir.key_ranges.write().unwrap();
                    let mut new_hwm = hwm;
                    for (idx, changes) in changesets {
                        if idx != new_hwm + 1 {
                            break; // gap — wait for eventual consistency
                        }
                        // Deletes first, then inserts.
                        for change in &changes {
                            if let ShardDirectoryChange::TombstoneShard { shard } = change {
                                ranges.remove(shard);
                            }
                        }
                        for change in changes {
                            if let ShardDirectoryChange::ActivateShard { shard, range, .. } = change {
                                ranges.insert(shard, range);
                            }
                        }
                        new_hwm = idx;
                    }
                    drop(ranges);
                    *dir.route_hwm.write().unwrap() = new_hwm;
                }
                let route_ms = route_start.elapsed().as_millis();

                // PUSH: push only own_shards entries to remote.
                // Prevents echo traffic (pulled entries pushed back).
                // Remote rejects stale pushes (view < current) — harmless.
                // Remote tombstones reject puts for decommissioned shards — harmless.
                let push_start = std::time::Instant::now();
                let own = dir.own_shards.read().unwrap().clone();
                for (shard, membership, local_view) in dir.local.all() {
                    if own.contains(&shard) {
                        let _ = dir.remote.strong_put_active_shard_view_membership(shard, membership, local_view).await;
                    }
                }
                let push_ms = push_start.elapsed().as_millis();

                let total_ms = cycle_wall_start.elapsed().as_millis();
                cycle_count += 1;
                if total_ms > 500 || cycle_count.is_multiple_of(50) {
                    eprintln!(
                        "[csd] sync cycle={cycle_count} \
                         pull={pull_ms}ms route={route_ms}ms push={push_ms}ms \
                         total={total_ms}ms own_shards={}",
                        own.len(),
                    );
                }
            }
        });

        dir
    }

    /// Return the current cached key ranges (pulled from remote).
    pub fn key_ranges(&self) -> BTreeMap<ShardNumber, KeyRange<K>> {
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

    use crate::testing::{
        discovery::build_single_node_discovery,
        test_rng,
    };

    async fn remote_put(
        r: &impl RemoteShardDirectory<usize, ()>,
        shard: ShardNumber,
        membership: IrMembership<usize>,
        view: u64,
    ) {
        r.strong_put_active_shard_view_membership(shard, membership, view).await.unwrap();
    }

    async fn remote_get(
        r: &impl RemoteShardDirectory<usize, ()>,
        shard: ShardNumber,
    ) -> Option<ShardRecord<usize, ()>> {
        r.strong_get_shard(shard).await.unwrap()
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn caching_directory_local_operations() {
        let mut rng = test_rng(42);
        let disc = build_single_node_discovery(&mut rng);
        let remote = disc.create_remote(&mut rng);
        let local = Arc::new(InMemoryShardDirectory::new());
        let dir = CachingShardDirectory::<usize, (), _>::new(local, remote, Duration::from_secs(60));

        // Local put/get work immediately.
        let shard = ShardNumber(1);
        let membership = IrMembership::new(vec![10, 20, 30]);
        dir.put(shard, membership, 0);
        let (got, _) = dir.get(shard).unwrap();
        assert_eq!(got.len(), 3);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn caching_directory_push_to_remote() {
        let mut rng = test_rng(43);
        let disc = build_single_node_discovery(&mut rng);
        let remote = disc.create_remote(&mut rng);
        let remote_check = disc.create_remote(&mut rng);
        let local = Arc::new(InMemoryShardDirectory::new());
        let dir =
            CachingShardDirectory::<usize, (), _>::new(local, remote, Duration::from_millis(100));

        let shard = ShardNumber(1);
        dir.add_own_shard(shard);
        dir.put(shard, IrMembership::new(vec![10, 20, 30]), 0);

        // Advance time past one sync cycle.
        tokio::time::sleep(Duration::from_millis(150)).await;
        // Yield to let the background task run.
        tokio::task::yield_now().await;

        let pushed = remote_get(&*remote_check, ShardNumber(1)).await.unwrap();
        assert_eq!(pushed.membership.len(), 3);
        assert!(pushed.membership.contains(10));
        assert!(pushed.membership.contains(20));
        assert!(pushed.membership.contains(30));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn caching_directory_pull_from_remote() {
        let mut rng = test_rng(44);
        let disc = build_single_node_discovery(&mut rng);
        let remote_setup = disc.create_remote(&mut rng);
        let remote = disc.create_remote(&mut rng);

        // Another node registered shard 2 directly.
        remote_put(&*remote_setup, ShardNumber(2), IrMembership::new(vec![40, 50]), 0).await;

        let local = Arc::new(InMemoryShardDirectory::new());
        let dir =
            CachingShardDirectory::<usize, (), _>::new(local, remote, Duration::from_millis(100));

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
    async fn caching_directory_monotonic_pull_rejects_stale() {
        let mut rng = test_rng(46);
        let disc = build_single_node_discovery(&mut rng);
        let remote_setup = disc.create_remote(&mut rng);
        let remote = disc.create_remote(&mut rng);
        let local = Arc::new(InMemoryShardDirectory::new());
        let dir =
            CachingShardDirectory::<usize, (), _>::new(local, remote, Duration::from_millis(100));

        // Register shard 1 locally at view 5 (simulates alive replica after view change).
        dir.put(ShardNumber(1), IrMembership::new(vec![10, 20]), 5);

        // Simulate stale remote state for the same shard at view 3.
        remote_put(&*remote_setup, ShardNumber(1), IrMembership::new(vec![99]), 3).await;

        // After sync, PULL writes remote entry but put() rejects it (3 < 5).
        tokio::time::sleep(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;

        let (got, view) = dir.get(ShardNumber(1)).unwrap();
        assert_eq!(got.len(), 2); // Still the local value, not the remote "99".
        assert_eq!(view, 5);
    }
}
