use super::{
    dynamic_router::ShardEntry, Change, Key, KeyRange, ShardNumber, Timestamp, Value,
};
use crate::tapir::shard_manager::ShardManager;
use crate::tapir::{Replica, Sharded};
use crate::transport::Transport;
use crate::{IrClientId, IrMembership, OccTransaction, OccTransactionId};
use std::collections::HashMap;
use tracing::info;

impl<K: Key + Clone, V: Value + Clone, T: Transport<Replica<K, V>>> ShardManager<K, V, T> {
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
        let (changes, mut last_validated) = self.shards[&source]
            .client
            .scan_changes(0, u64::MAX)
            .await;
        let filtered = filter_changes(&changes, &split_key);
        info!("split: bulk copied {} changes to new shard", filtered.len());
        ship_changes(&self.shards[&new_shard].client, new_shard, &filtered).await;

        // Phase 2: Catch-up tailing.
        loop {
            let (changes, new_validated) = self.shards[&source]
                .client
                .scan_changes(last_validated.saturating_add(1), u64::MAX)
                .await;
            let filtered = filter_changes(&changes, &split_key);
            if !filtered.is_empty() {
                ship_changes(&self.shards[&new_shard].client, new_shard, &filtered).await;
            }
            if new_validated == last_validated {
                break;
            }
            last_validated = new_validated;
        }

        // Phase 3: Atomic cutover — narrow the source's key range.
        let narrowed_range = KeyRange {
            start: source_range.start.clone(),
            end: Some(split_key.clone()),
        };
        let config = serde_json::to_vec(&narrowed_range).expect("serialize key range");
        self.shards[&source].client.reconfigure(config);
        info!("split: reconfigured source shard, draining...");

        // Poll until validated_timestamp catches up (pending prepares drain).
        loop {
            let (changes, new_validated) = self.shards[&source]
                .client
                .scan_changes(last_validated.saturating_add(1), u64::MAX)
                .await;
            let filtered = filter_changes(&changes, &split_key);
            if !filtered.is_empty() {
                ship_changes(&self.shards[&new_shard].client, new_shard, &filtered).await;
            }
            if new_validated == last_validated {
                break;
            }
            last_validated = new_validated;
        }

        // Reconfigure the new shard with its key range.
        let new_config = serde_json::to_vec(&new_range).expect("serialize key range");
        self.shards[&new_shard].client.reconfigure(new_config);

        // Update the source shard's key range in our registry.
        if let Some(managed) = self.shards.get_mut(&source) {
            managed.key_range = narrowed_range.clone();
        }

        // Update directory.
        let mut dir = self.directory.write().unwrap();
        dir.update(vec![
            ShardEntry { shard: source, range: narrowed_range },
            ShardEntry { shard: new_shard, range: new_range },
        ]);
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
