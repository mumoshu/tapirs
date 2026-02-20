use super::{
    dynamic_router::{ShardDirectory, ShardEntry},
    Key, KeyRange, ShardClient, ShardNumber, Value,
};
use crate::discovery::ShardDirectory as AddressDirectory;
use crate::transport::Transport;
use crate::tapir::Replica;
use crate::{IrClientId, IrMembership};
use std::collections::HashMap;

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
///   InMemoryRemoteDirectory / HttpDiscoveryServer
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
    pub client: ShardClient<K, V, T>,
}

/// Sidecar that orchestrates resharding operations (split, merge, migrate)
/// using `IrClient` to interact with shard groups via the normal protocol.
/// NOT in the message path — uses the same API as application clients.
pub struct ShardManager<K: Key, V: Value, T: Transport<Replica<K, V>>, D: AddressDirectory<T::Address>> {
    pub(crate) shards: HashMap<ShardNumber, ManagedShard<K, V, T>>,
    pub(crate) directory: ShardDirectory<K>,
    pub(crate) address_directory: D,
    pub(crate) transport: T,
    client_id: IrClientId,
    pub(crate) rng: crate::Rng,
    pub(crate) on_progress: Option<Box<dyn Fn(&str) + Send + Sync>>,
}

impl<K: Key, V: Value, T: Transport<Replica<K, V>>, D: AddressDirectory<T::Address>> ShardManager<K, V, T, D> {
    pub fn new(
        mut rng: crate::Rng,
        transport: T,
        address_directory: D,
    ) -> Self {
        let client_id = IrClientId::new(&mut rng);
        Self {
            shards: HashMap::new(),
            directory: ShardDirectory::new(vec![]),
            address_directory,
            transport: transport.clone(),
            client_id,
            rng,
            on_progress: None,
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

    pub fn register_shard(
        &mut self,
        shard: ShardNumber,
        membership: IrMembership<T::Address>,
        key_range: KeyRange<K>,
    ) {
        self.address_directory.put(shard, membership.clone(), 0);
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
            client,
        });
        self.rebuild_directory();
    }

    /// Create a shard client and insert into `self.shards` without touching
    /// the address directory or rebuilding the key-range directory.
    ///
    /// Used by compact to set up the new shard's client during migration
    /// without making it visible to cross-shard discovery.
    pub(crate) fn create_shard_client(
        &mut self,
        shard: ShardNumber,
        membership: IrMembership<T::Address>,
        key_range: KeyRange<K>,
    ) {
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
            client,
        });
    }

    /// Atomically replace one shard with another in all directories.
    ///
    /// Removes `old` from the local shard registry, performs an atomic swap
    /// in the address directory (cross-shard discovery), and swaps the shard
    /// number in-place in the key-range directory (no full rebuild needed
    /// since key ranges are unchanged).
    ///
    /// Caller MUST tombstone old shard in remote discovery to prevent
    /// push-pull cycles. See [`ShardManager`] module docs § "Careful
    /// Handling: Avoiding Push-Pull Cycles".
    pub(crate) fn replace_shard(
        &mut self,
        old: ShardNumber,
        new: ShardNumber,
        membership: IrMembership<T::Address>,
    ) {
        self.shards.remove(&old);
        self.address_directory.replace(old, new, membership, 0);
        self.directory.replace(old, new);
    }

    /// Removes shard from local directories.
    ///
    /// Caller MUST tombstone in remote discovery to prevent push-pull
    /// cycles. See [`ShardManager`] module docs § "Careful Handling:
    /// Avoiding Push-Pull Cycles".
    pub fn deregister_shard(&mut self, shard: ShardNumber) {
        self.shards.remove(&shard);
        self.address_directory.remove(shard);
        self.rebuild_directory();
    }

    pub fn shard_client(&self, shard: ShardNumber) -> Option<&ShardClient<K, V, T>> {
        self.shards.get(&shard).map(|s| &s.client)
    }

    #[allow(clippy::disallowed_methods)] // values() order is irrelevant — ShardDirectory sorts internally
    pub(crate) fn rebuild_directory(&mut self) {
        let entries = self
            .shards
            .values()
            .map(|s| ShardEntry {
                shard: s.shard,
                range: s.key_range.clone(),
            })
            .collect();
        self.directory = ShardDirectory::new(entries);
    }
}
