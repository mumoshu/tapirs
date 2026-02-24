use super::{
    dynamic_router::ShardEntry,
    Key, KeyRange, ShardClient, ShardNumber, Value,
};
use crate::discovery::{RemoteShardDirectory, ShardDirectoryChange, ShardRecord, ShardStatus};
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
/// ShardManager is the sole authority for shard **lifecycle** (register,
/// compact, split, merge). Lifecycle methods tombstone old shards in the
/// remote discovery service via `strong_atomic_update_shards`, ensuring
/// cluster-wide consistency.
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
///   strong_atomic_update_shards tombstones old shards in remote directly
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
/// | Merge     | Tombstoned via route update | Surviving keeps its #          | No          |
/// | Compact   | Tombstoned via route update | Fresh # (operator-specified)   | Yes (v=0)   |
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

    /// Create a `ShardClient` using the manager's shared client ID, RNG, and
    /// transport. Does not store the client — callers use it as a local variable.
    pub(crate) fn make_shard_client(
        &mut self,
        shard: ShardNumber,
        membership: IrMembership<T::Address>,
    ) -> ShardClient<K, V, T> {
        ShardClient::new(
            self.rng.fork(),
            self.client_id,
            shard,
            membership,
            self.transport.clone(),
        )
    }

    /// Query remote discovery for an active shard's record.
    ///
    /// Returns `ShardRecord` if the shard exists and is `Active`.
    /// Returns `ShardNotRegistered` if not found or not active.
    /// Returns `DiscoveryError` on network/protocol failure.
    pub(crate) async fn get_active_shard(
        &self,
        shard: ShardNumber,
    ) -> Result<ShardRecord<T::Address, K>, super::shard_manager_cdc::ReshardError> {
        use super::shard_manager_cdc::ReshardError;
        let record = self.remote.strong_get_shard(shard).await
            .map_err(|e| ReshardError::DiscoveryError(format!("{shard:?}: {e:?}")))?
            .ok_or(ReshardError::ShardNotRegistered(shard))?;
        if record.status != ShardStatus::Active {
            return Err(ReshardError::ShardNotRegistered(shard));
        }
        Ok(record)
    }

    /// Registers an already configured and running shard's replicas with the
    /// discovery cluster, making them visible to clients and other replicas.
    ///
    /// This is only used in setups that have a discovery TAPIR cluster.
    /// It is not used when nodes use static membership (via `--discovery-json`),
    /// which is the typical configuration for the discovery cluster itself.
    pub async fn register_active_shard(
        &mut self,
        shard: ShardNumber,
        membership: IrMembership<T::Address>,
        key_range: KeyRange<K>,
    ) {
        let _ = self.remote.strong_atomic_update_shards(vec![
            ShardDirectoryChange::ActivateShard {
                shard,
                range: key_range.clone(),
                membership: membership.clone(),
                view: 0,
            },
        ]).await;
        let client = self.make_shard_client(shard, membership.clone());
        self.shards.insert(shard, ManagedShard {
            shard,
            key_range,
            membership,
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
        let client = self.make_shard_client(shard, membership.clone());
        self.shards.insert(shard, ManagedShard {
            shard,
            key_range,
            membership,
            client,
        });
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
