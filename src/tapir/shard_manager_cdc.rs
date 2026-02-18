use super::replica::ShardConfig;
use super::{Change, Key, KeyRange, ShardNumber, Value};
use crate::discovery::ShardDirectory as AddressDirectory;
use crate::tapir::shard_manager::ShardManager;
use crate::tapir::{Replica, Sharded};
use crate::transport::Transport;
use crate::{IrClientId, IrMembership, OccTransaction, OccTransactionId};
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::info;

impl<K: Key + Clone, V: Value + Clone, T: Transport<Replica<K, V>>, D: AddressDirectory<T::Address>> ShardManager<K, V, T, D> {
    /// Split a shard at `split_key`: keys < split_key stay on `source`,
    /// keys >= split_key move to `new_shard`.
    pub async fn split(
        &mut self,
        source: ShardNumber,
        split_key: K,
        new_shard: ShardNumber,
        new_membership: IrMembership<T::Address>,
    ) {
        let source_range = self.shards.get(&source)
            .expect("source shard not registered")
            .key_range
            .clone();

        let new_range = KeyRange {
            start: Some(split_key.clone()),
            end: source_range.end.clone(),
        };
        let narrowed_range = KeyRange {
            start: source_range.start.clone(),
            end: Some(split_key.clone()),
        };

        // Update source's key range in our map before registering the new shard
        // to avoid overlapping ranges in the directory.
        if let Some(managed) = self.shards.get_mut(&source) {
            managed.key_range = narrowed_range.clone();
        }
        self.register_shard(new_shard, new_membership, new_range.clone());

        // Phase 1: Bulk copy via scan_changes.
        let r = self.shards[&source].client.scan_changes(0).await;
        let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
        let filtered = filter_changes(&changes, &split_key);
        info!("split: bulk copied {} changes to new shard", filtered.len());
        ship_changes(&self.shards[&new_shard].client, new_shard, &filtered, &mut self.rng).await;
        let mut last_view = r.effective_end_view;

        // Phase 2: Catch-up tailing.
        loop {
            let r = self.shards[&source]
                .client
                .scan_changes(last_view + 1)
                .await;
            let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
            let filtered = filter_changes(&changes, &split_key);
            if !filtered.is_empty() {
                ship_changes(&self.shards[&new_shard].client, new_shard, &filtered, &mut self.rng).await;
            }
            if r.effective_end_view == last_view {
                break;
            }
            last_view = r.effective_end_view;
        }

        // Phase 3a: Freeze source — reject all Prepare with Fail.
        let freeze = serde_json::to_vec(&ShardConfig::<K> {
            key_range: None,
            read_only: true,
        })
        .expect("serialize freeze config");
        self.shards[&source].client.reconfigure(freeze);
        info!("split: source frozen, draining...");

        // Phase 3b: Drain — wait for ALL prepared transactions to resolve + final seal.
        //
        // After freeze: no new CO::Prepare accepted (read_only=true).
        // Existing prepares resolve via tick() -> recover_coordination().
        // IO::Commit for each resolution is captured in the current view.
        // Need one more view change after all prepares resolve to seal final commits.
        let narrowed_range = KeyRange {
            start: source_range.start.clone(),
            end: Some(split_key.clone()),
        };
        loop {
            T::sleep(Duration::from_secs(1)).await;
            let r = self.shards[&source]
                .client
                .scan_changes(last_view + 1)
                .await;
            let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
            let filtered = filter_changes(&changes, &split_key);
            if !filtered.is_empty() {
                ship_changes(&self.shards[&new_shard].client, new_shard, &filtered, &mut self.rng).await;
            }
            last_view = last_view.max(r.effective_end_view);

            if r.pending_prepares == 0 && changes.is_empty() && r.effective_end_view == last_view {
                break;
            }
        }
        // One final poll after all prepares resolved to capture sealed commits.
        T::sleep(Duration::from_secs(3)).await;
        let r = self.shards[&source]
            .client
            .scan_changes(last_view + 1)
            .await;
        let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
        let filtered = filter_changes(&changes, &split_key);
        if !filtered.is_empty() {
            ship_changes(&self.shards[&new_shard].client, new_shard, &filtered, &mut self.rng).await;
        }

        // Phase 3c: Unfreeze source and narrow its key range.
        let unfreeze = serde_json::to_vec(&ShardConfig {
            key_range: Some(narrowed_range.clone()),
            read_only: false,
        })
        .expect("serialize unfreeze config");
        self.shards[&source].client.reconfigure(unfreeze);

        // Reconfigure the new shard with its key range.
        let new_config = serde_json::to_vec(&new_range).expect("serialize key range");
        self.shards[&new_shard].client.reconfigure(new_config);

        // Update the source shard's key range in our registry.
        if let Some(managed) = self.shards.get_mut(&source) {
            managed.key_range = narrowed_range.clone();
        }

        // Rebuild directory from all registered shards (not just source + new).
        self.rebuild_directory();
        info!("split: complete");
    }
    /// Merge two adjacent shards: `absorbed` is removed, its data shipped to `surviving`.
    ///
    /// # Algorithm (mirrors split's 3-phase approach)
    ///
    /// **Validation**: Both shards must exist with adjacent key ranges. The merged
    /// range is computed via `KeyRange::union()`.
    ///
    /// **Phase 1 — Bulk Copy**: `scan_changes(0)` on the absorbed shard, ship ALL
    /// changes to the surviving shard (no key filtering — all data moves).
    ///
    /// **Phase 2 — Catch-up Tailing**: Loop `scan_changes(last_view + 1)` on the
    /// absorbed shard. Ship new changes to the surviving shard. Stop when
    /// `effective_end_view` stabilizes.
    ///
    /// **Phase 3a — Freeze Absorbed**: `reconfigure(ShardConfig { read_only: true })`.
    /// New Prepare returns Fail. Existing prepared transactions resolve via
    /// `recover_coordination()` tick.
    ///
    /// **Phase 3b — Drain Prepared on Absorbed**: Wait for `pending_prepares == 0`
    /// (using the `any(pp == 0)` check — by quorum intersection, a replica with
    /// pp=0 is authoritative). Continue shipping remaining changes. Final 3-second
    /// sleep + one more poll to capture sealed commits.
    ///
    /// **Phase 3c — Expand Surviving + Cleanup**:
    /// 1. `reconfigure(ShardConfig { key_range: merged_range })` on surviving
    /// 2. Sleep 5 seconds for view change to propagate
    /// 3. Update surviving shard's `key_range` in `self.shards`
    /// 4. `deregister_shard(absorbed)` — removes from shards, address_directory,
    ///    rebuilds directory
    ///
    /// # Key design decisions
    ///
    /// - **No key filtering**: Unlike split which filters `key >= split_key`, merge
    ///   ships ALL changes from the absorbed shard.
    /// - **Absorbed shard is NOT unfrozen**: It's being removed entirely. The freeze
    ///   is permanent.
    /// - **Surviving shard is NOT frozen**: Continues operating normally. Safe because
    ///   key ranges are disjoint — the surviving shard's OCC validates keys in its
    ///   range only, shipped commits write keys in the absorbed range only, and no
    ///   client routes absorbed-range keys to the surviving shard until after
    ///   expansion.
    /// - **IO::Commit has no key_range check**: The surviving shard accepts shipped
    ///   commits for keys outside its current range. So we can ship data throughout
    ///   phases 1–3b before expanding the range in 3c.
    ///
    /// # Downtime analysis
    ///
    /// `read_only` is ONLY checked in `exec_consensus` for `CO::Prepare`. All
    /// unlogged and inconsistent operations (`UO::Get`, `UO::Scan`,
    /// `IO::QuorumRead`, `IO::QuorumScan`, `IO::Commit`, `IO::Abort`) do NOT
    /// check `read_only`.
    ///
    /// | Phase | RW txns (absorbed range) | RO txns (absorbed range) | All txns (surviving range) |
    /// |-------|--------------------------|--------------------------|----------------------------|
    /// | Phase 1-2 (bulk copy + tailing) | Normal | Normal | Normal |
    /// | Phase 3a-3b (freeze + drain) | DOWN — Prepare returns Fail | Normal — reads work | Normal |
    /// | Phase 3c reconfigure surviving | DOWN — Prepare returns Fail | Normal — reads work | Brief disruption (~2-5s view change) |
    /// | Phase 3c deregister absorbed | DOWN — connection errors | DOWN — connection errors | Normal (expanded range) |
    /// | After directory refresh | Normal (routed to surviving) | Normal (routed to surviving) | Normal |
    ///
    /// # CDC cursor and `pending_prepares` semantics
    ///
    /// **Cursor advancement**: `effective_end_view` is the highest `base_view` key
    /// in the replica's `record_delta_during_view: BTreeMap<u64, LeaderRecordDelta>`.
    /// This is a monotonic cursor. The drain loop advances it via
    /// `last_view = last_view.max(r.effective_end_view)`. If multiple view changes
    /// happen between scans, a single `scan_changes(last_view + 1)` returns deltas
    /// for all of them.
    ///
    /// **`pending_prepares` scope**: `self.inner.prepared.len()` — count of ALL
    /// entries in the OCC store's prepared HashMap. View-agnostic. Entries added on
    /// Prepare, removed on Commit/Abort.
    ///
    /// **"Last mile" commits**: When `pending_prepares` reaches 0, resolved
    /// transactions' IO::Commit/Abort are in the IR record but no delta exists yet
    /// (requires view change). The 3-second sleep covers this: IR tick fires every
    /// 2s, first tick skipped by `changed_view_recently`, second tick (~4s after
    /// freeze) triggers a natural view change sealing the commits. The final
    /// `scan_changes` poll picks up this delta.
    pub async fn merge(
        &mut self,
        absorbed: ShardNumber,
        surviving: ShardNumber,
    ) {
        let absorbed_range = self.shards.get(&absorbed)
            .expect("absorbed shard not registered")
            .key_range
            .clone();
        let surviving_range = self.shards.get(&surviving)
            .expect("surviving shard not registered")
            .key_range
            .clone();
        assert!(
            absorbed_range.adjacent(&surviving_range),
            "shard ranges must be adjacent to merge"
        );
        let merged_range = surviving_range.union(&absorbed_range);

        // Phase 1: Bulk copy — ship all changes from absorbed to surviving.
        let r = self.shards[&absorbed].client.scan_changes(0).await;
        let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
        info!("merge: bulk copied {} changes to surviving shard", changes.len());
        ship_changes(&self.shards[&surviving].client, surviving, &changes, &mut self.rng).await;
        let mut last_view = r.effective_end_view;

        // Phase 2: Catch-up tailing.
        loop {
            let r = self.shards[&absorbed]
                .client
                .scan_changes(last_view + 1)
                .await;
            let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
            if !changes.is_empty() {
                ship_changes(&self.shards[&surviving].client, surviving, &changes, &mut self.rng).await;
            }
            if r.effective_end_view == last_view {
                break;
            }
            last_view = r.effective_end_view;
        }

        // Phase 3a: Freeze absorbed — reject all Prepare with Fail.
        let freeze = serde_json::to_vec(&ShardConfig::<K> {
            key_range: None,
            read_only: true,
        })
        .expect("serialize freeze config");
        self.shards[&absorbed].client.reconfigure(freeze);
        info!("merge: absorbed shard frozen, draining...");

        // Phase 3b: Drain — wait for pending_prepares == 0 + final seal.
        loop {
            T::sleep(Duration::from_secs(1)).await;
            let r = self.shards[&absorbed]
                .client
                .scan_changes(last_view + 1)
                .await;
            let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
            if !changes.is_empty() {
                ship_changes(&self.shards[&surviving].client, surviving, &changes, &mut self.rng).await;
            }
            last_view = last_view.max(r.effective_end_view);

            if r.pending_prepares == 0 && changes.is_empty() && r.effective_end_view == last_view {
                break;
            }
        }
        // One final poll after all prepares resolved to capture sealed commits.
        T::sleep(Duration::from_secs(3)).await;
        let r = self.shards[&absorbed]
            .client
            .scan_changes(last_view + 1)
            .await;
        let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
        if !changes.is_empty() {
            ship_changes(&self.shards[&surviving].client, surviving, &changes, &mut self.rng).await;
        }

        // Phase 3c: Expand surviving shard's key range.
        let expand = serde_json::to_vec(&ShardConfig {
            key_range: Some(merged_range.clone()),
            read_only: false,
        })
        .expect("serialize expanded config");
        self.shards[&surviving].client.reconfigure(expand);

        // Wait for view change to propagate.
        T::sleep(Duration::from_secs(5)).await;

        // Update surviving shard's key range in our registry.
        if let Some(managed) = self.shards.get_mut(&surviving) {
            managed.key_range = merged_range;
        }

        // Remove absorbed shard and rebuild directory.
        self.deregister_shard(absorbed);
        info!("merge: complete");
    }
}

/// Filter changes to those with key >= split_key.
fn filter_changes<K: Ord + Clone, V: Clone>(
    changes: &[Change<K, V>],
    split_key: &K,
) -> Vec<Change<K, V>> {
    changes
        .iter()
        .filter(|c| c.key >= *split_key)
        .cloned()
        .collect()
}

/// Ship a set of changes to a target shard by wrapping each as an IO::Commit.
async fn ship_changes<K: Key + Clone, V: Value + Clone, T: Transport<Replica<K, V>>>(
    client: &super::ShardClient<K, V, T>,
    shard: ShardNumber,
    changes: &[Change<K, V>],
    rng: &mut crate::Rng,
) {
    for change in changes {
        let id = OccTransactionId {
            client_id: IrClientId::new(rng),
            number: 0,
        };
        let key = Sharded { shard, key: change.key.clone() };
        let mut write_set = BTreeMap::new();
        write_set.insert(key, change.value.clone());
        let txn = OccTransaction {
            read_set: BTreeMap::new(),
            write_set,
            scan_set: Vec::new(),
        };
        client.commit(id, txn, change.timestamp).await;
    }
}
