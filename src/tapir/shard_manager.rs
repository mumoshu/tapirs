use super::{
    dynamic_router::ShardEntry,
    Key, KeyRange, ShardClient, ShardNumber, Value,
};
use crate::discovery::{RemoteShardDirectory, ShardDirectoryChange};
use crate::transport::Transport;
use crate::tapir::Replica;
use crate::{IrClientId, IrMembership};
use std::collections::HashMap;
use std::sync::Arc;

/// Shard lifecycle manager for TAPIR clusters.
///
/// # Membership Authority Chain
///
/// Replicas are the authoritative source for their shard's membership:
/// 1. IR view change consensus → `view.membership` is the canonical membership
/// 2. `Transport::on_membership_changed()` writes to node-local `InMemoryShardDirectory`
///    (instant, synchronous)
/// 3. `CachingShardDirectory` PUSH syncs own-shard entries to remote discovery
///    (periodic; remote rejects stale via view numbers)
/// 4. Other nodes' `CachingShardDirectory` PULL from remote discovery
///    (periodic; local rejects stale via view numbers)
///
/// ShardManager is the sole authority for shard **lifecycle** (register, deregister,
/// replace). Lifecycle methods (`deregister_shard`, `replace_shard`) tombstone old
/// shards in the remote discovery service, ensuring cluster-wide consistency.
///
/// # Architecture
///
/// ```text
/// Per-Node (may host replicas from multiple shards):
///   InMemoryShardDirectory (local)
///     ← Channel reads shard_addresses() from here
///     ← on_membership_changed() writes here (from any replica on the node)
///   CachingShardDirectory(local, remote, sync_interval)
///     - PUSH: own_shards entries → remote (remote rejects stale via view)
///     - PULL: all remote entries → local (local rejects stale via view)
///
/// ShardManager (shard lifecycle authority):
///   deregister_shard/replace_shard tombstone old shards in remote directly
///
/// Remote Discovery (cluster-wide):
///   TapirRemoteShardDirectory / JsonRemoteShardDirectory
///     - Stores (shard, membership, view) entries
///     - Tombstones removed/replaced shards (rejects future puts)
///     - all() omits tombstoned entries
/// ```
///
/// # Shard Lifecycle in Resharding Operations
///
/// | Operation | Old shard               | New shard                      | View reset? |
/// |-----------|-------------------------|--------------------------------|-------------|
/// | Split     | Kept (range narrowed)   | Fresh # (operator-specified)   | No          |
/// | Merge     | `deregister_shard()`    | Surviving keeps its #          | No          |
/// | Compact   | `replace_shard()`       | Fresh # (operator-specified)   | Yes (v=0)   |
/// | Join/Leave| Membership updated      | N/A                            | No          |
///
/// Shard numbers are never reused. Remote tombstones enforce this cluster-wide.
///
/// # Membership Change Authority
///
/// ShardManager is the sole entity that sends `AddMember`/`RemoveMember`
/// to IR replicas (via `join()` and `leave()`). IR view changes without
/// membership change (e.g., suspected dead leader) only advance the view
/// number — the same replicas remain in the membership.
///
/// **Operational invariant**: All membership changes MUST go through
/// ShardManager. Sending `AddMember`/`RemoveMember` directly to replicas
/// (bypassing ShardManager) would create stale directories — ShardManager
/// would not know the updated membership, and other nodes relying on
/// ShardManager-seeded directories would have stale addresses.
///
/// Because ShardManager initiates all membership changes, its local
/// directory is always current — no background discovery sync (e.g.,
/// `CachingShardDirectory`) is needed for ShardManager itself.
///
/// # Careful Handling: Avoiding Push-Pull Cycles
///
/// Without remote tombstones, any unilateral local removal can be undone by
/// the PUSH/PULL sync cycle:
///
/// 1. Node A removes shard locally → next PULL re-inserts from remote
/// 2. Node B still has stale shard → PUSH re-inserts into remote after
///    Node A's remote.remove()
///
/// Remote tombstones break this cycle: once a shard is tombstoned, `put()`
/// for that shard is permanently rejected, and `all()` omits it. View-based
/// monotonic writes (`put()` rejects if view < current) prevent stale
/// membership from overwriting fresh data in both directions.
///
/// `own_shards` in CachingShardDirectory provides a PUSH filter — nodes only
/// push membership for shards they host replicas for, avoiding O(N×S) echo
/// traffic. PULL is never filtered — monotonic writes protect local data.
///
/// A shard group managed by the ShardManager.
pub struct ManagedShard<K: Key, V: Value, T: Transport<Replica<K, V>>> {
    pub shard: ShardNumber,
    pub key_range: KeyRange<K>,
    pub membership: IrMembership<T::Address>,
    pub client: ShardClient<K, V, T>,
}

/// Sidecar that orchestrates resharding operations (split, merge, migrate)
/// using `IrClient` to interact with shard groups via the normal protocol.
/// NOT in the message path — uses the same API as application clients.
pub struct ShardManager<
    K: Key,
    V: Value,
    T: Transport<Replica<K, V>>,
    RD: RemoteShardDirectory<T::Address, K>,
> {
    pub(crate) shards: HashMap<ShardNumber, ManagedShard<K, V, T>>,
    pub(crate) transport: T,
    client_id: IrClientId,
    pub(crate) rng: crate::Rng,
    pub(crate) on_progress: Option<Box<dyn Fn(&str) + Send + Sync>>,
    pub(crate) remote: Arc<RD>,
}

impl<
    K: Key,
    V: Value,
    T: Transport<Replica<K, V>>,
    RD: RemoteShardDirectory<T::Address, K>,
> ShardManager<K, V, T, RD>
{
    pub fn new(
        mut rng: crate::Rng,
        transport: T,
        remote: Arc<RD>,
    ) -> Self {
        let client_id = IrClientId::new(&mut rng);
        Self {
            shards: HashMap::new(),
            transport: transport.clone(),
            client_id,
            rng,
            on_progress: None,
            remote,
        }
    }

    pub(crate) fn set_progress_callback(&mut self, cb: impl Fn(&str) + Send + Sync + 'static) {
        self.on_progress = Some(Box::new(cb));
    }

    pub(crate) fn report_progress(&self, msg: &str) {
        if let Some(ref cb) = self.on_progress {
            cb(msg);
        }
    }

    pub async fn register_shard(
        &mut self,
        shard: ShardNumber,
        membership: IrMembership<T::Address>,
        key_range: KeyRange<K>,
    ) {
        let _ = self.remote.strong_publish_route_changes(vec![
            ShardDirectoryChange::SetRange {
                shard,
                range: key_range.clone(),
                membership: membership.clone(),
                view: 0,
            },
        ]).await;
        let stored_membership = membership.clone();
        let client = ShardClient::new(
            self.rng.fork(),
            self.client_id,
            shard,
            membership,
            self.transport.clone(),
        );
        self.shards.insert(shard, ManagedShard {
            shard,
            key_range,
            membership: stored_membership,
            client,
        });
    }

    /// Create a shard client and insert into `self.shards` without touching
    /// remote discovery.
    ///
    /// Used by compact to set up the new shard's client during migration
    /// without making it visible to cross-shard discovery.
    pub(crate) fn create_shard_client(
        &mut self,
        shard: ShardNumber,
        membership: IrMembership<T::Address>,
        key_range: KeyRange<K>,
    ) {
        let stored_membership = membership.clone();
        let client = ShardClient::new(
            self.rng.fork(),
            self.client_id,
            shard,
            membership,
            self.transport.clone(),
        );
        self.shards.insert(shard, ManagedShard {
            shard,
            key_range,
            membership: stored_membership,
            client,
        });
    }

    /// Atomically replace one shard with another in remote discovery and
    /// tombstone the old shard.
    ///
    /// Removes `old` from the local shard registry and tombstones it in
    /// remote discovery, preventing other nodes from re-pushing stale data.
    ///
    /// See [`ShardManager`] module docs § "Careful Handling: Avoiding
    /// Push-Pull Cycles".
    pub(crate) async fn replace_shard(
        &mut self,
        old: ShardNumber,
        new: ShardNumber,
        membership: IrMembership<T::Address>,
    ) {
        self.shards.remove(&old);
        let _ = self.remote.strong_replace(old, new, membership, 0).await;
    }

    /// Removes shard from local registry and tombstones in remote
    /// discovery.
    ///
    /// See [`ShardManager`] module docs § "Careful Handling: Avoiding
    /// Push-Pull Cycles".
    pub async fn deregister_shard(&mut self, shard: ShardNumber) {
        self.shards.remove(&shard);
        let _ = self.remote.strong_remove(shard).await;
    }

    pub fn shard_client(&self, shard: ShardNumber) -> Option<&ShardClient<K, V, T>> {
        self.shards.get(&shard).map(|s| &s.client)
    }

    /// Return the current shard topology (shard number + key range).
    #[allow(clippy::disallowed_methods)] // values() order is irrelevant — callers sort if needed
    pub fn shard_entries(&self) -> Vec<ShardEntry<K>>
    where
        K: Clone,
    {
        self.shards
            .values()
            .map(|s| ShardEntry {
                shard: s.shard,
                range: s.key_range.clone(),
            })
            .collect()
    }

}
