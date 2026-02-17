use super::replica::ShardConfig;
use super::{Change, Key, KeyRange, ShardNumber, Value};
use crate::discovery::ShardDirectory as AddressDirectory;
use crate::tapir::shard_manager::ShardManager;
use crate::tapir::{Replica, Sharded};
use crate::transport::Transport;
use crate::{IrClientId, IrMembership, OccTransaction, OccTransactionId};
use std::collections::HashMap;
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
        self.register_shard(new_shard, new_membership, new_range.clone());

        // Phase 1: Bulk copy via scan_changes.
        let r = self.shards[&source].client.scan_changes(0).await;
        let changes: Vec<_> = r.deltas.into_iter().flat_map(|d| d.changes).collect();
        let filtered = filter_changes(&changes, &split_key);
        info!("split: bulk copied {} changes to new shard", filtered.len());
        ship_changes(&self.shards[&new_shard].client, new_shard, &filtered).await;
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
                ship_changes(&self.shards[&new_shard].client, new_shard, &filtered).await;
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
                ship_changes(&self.shards[&new_shard].client, new_shard, &filtered).await;
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
            ship_changes(&self.shards[&new_shard].client, new_shard, &filtered).await;
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
) {
    for change in changes {
        let id = OccTransactionId {
            client_id: IrClientId::new(),
            number: 0,
        };
        let key = Sharded { shard, key: change.key.clone() };
        let mut write_set = HashMap::new();
        write_set.insert(key, change.value.clone());
        let txn = OccTransaction {
            read_set: HashMap::new(),
            write_set,
            scan_set: Vec::new(),
        };
        client.commit(id, txn, change.timestamp).await;
    }
}
