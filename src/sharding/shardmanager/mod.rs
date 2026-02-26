#[allow(dead_code)]
pub(crate) mod cdc;
#[allow(dead_code)]
mod catchup;
pub mod scan_changes_types;

use crate::tapir::{Key, KeyRange, ShardClient, ShardNumber, Value};
use crate::discovery::{DiscoveryError, RemoteShardDirectory, ShardDirectoryChange, ShardRecord, ShardStatus};
use crate::transport::Transport;
use crate::tapir::Replica;
use crate::{IrClientId, IrMembership};
use std::marker::PhantomData;
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
/// # Stateless Design
///
/// ShardManager holds no local shard cache. All shard metadata (key range,
/// membership, status) is queried from the remote discovery cluster via
/// `strong_get_shard()` at the start of each operation. ShardClients are
/// created on-demand via `make_shard_client()` and used as local variables.
pub struct ShardManager<
    K: Key,
    V: Value,
    T: Transport<Replica<K, V>>,
    RD: RemoteShardDirectory<T::Address, K>,
> {
    pub(crate) transport: T,
    pub(crate) rng: crate::Rng,
    pub(crate) on_progress: Option<Box<dyn Fn(&str) + Send + Sync>>,
    pub(crate) remote: Arc<RD>,
    _phantom: PhantomData<(K, V)>,
}

impl<
    K: Key,
    V: Value,
    T: Transport<Replica<K, V>>,
    RD: RemoteShardDirectory<T::Address, K>,
> ShardManager<K, V, T, RD>
{
    pub fn new(
        rng: crate::Rng,
        transport: T,
        remote: Arc<RD>,
    ) -> Self {
        Self {
            transport: transport.clone(),
            rng,
            on_progress: None,
            remote,
            _phantom: PhantomData,
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

    /// Create a `ShardClient` with a fresh random client ID, the manager's RNG,
    /// and transport. Each call gets a unique ID so that op_id counters never
    /// collide when multiple ShardClients target the same shard across
    /// different resharding operations (e.g., split then merge).
    pub(crate) fn make_shard_client(
        &mut self,
        shard: ShardNumber,
        membership: IrMembership<T::Address>,
    ) -> ShardClient<K, V, T> {
        ShardClient::new(
            self.rng.fork(),
            IrClientId::new(&mut self.rng),
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
    ) -> Result<ShardRecord<T::Address, K>, cdc::ReshardError> {
        use cdc::ReshardError;
        let record = self.remote.strong_get_shard(shard).await
            .map_err(|e| ReshardError::DiscoveryError(format!("{shard:?}: {e:?}")))?
            .ok_or(ReshardError::ShardNotRegistered(shard))?;
        if record.status != ShardStatus::Active {
            return Err(ReshardError::ShardNotRegistered(shard));
        }
        Ok(record)
    }

    /// Ship CDC deltas to a shard via a ShardClient.
    ///
    /// Creates a ShardClient from the provided membership (same pattern as
    /// split Phase 1 bulk copy), then calls `ship_changes()` for each delta's
    /// changes. Used by the `/v1/apply-changes` endpoint during restore.
    pub(crate) async fn apply_changes(
        &mut self,
        shard: ShardNumber,
        membership: IrMembership<T::Address>,
        deltas: &[crate::tapir::LeaderRecordDelta<K, V>],
    ) {
        let client = self.make_shard_client(shard, membership);
        for delta in deltas {
            cdc::ship_changes(&client, shard, &delta.changes, &mut self.rng).await;
        }
    }

    /// Registers an already configured and running shard's replicas with the
    /// discovery cluster, making them visible to clients and other replicas.
    ///
    /// This is only used in setups that have a discovery TAPIR cluster.
    /// It is not used when nodes use static membership (via `--discovery-json`),
    /// which is the typical configuration for the discovery cluster itself.
    pub async fn register_active_shard(
        &self,
        shard: ShardNumber,
        membership: IrMembership<T::Address>,
        key_range: KeyRange<K>,
    ) -> Result<(), DiscoveryError> {
        self.remote.strong_atomic_update_shards(vec![
            ShardDirectoryChange::ActivateShard {
                shard,
                range: key_range,
                membership,
                view: 0,
            },
        ]).await
    }
}
