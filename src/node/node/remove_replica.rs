use super::*;

impl Node {
    pub fn remove_replica(&self, shard: ShardNumber) -> bool {
        let mut replicas = self.replicas.lock().unwrap();
        let removed = replicas.remove(&shard);
        if removed.is_some() {
            // Unregister from own_shards so CachingShardDirectory stops pushing for it.
            // Safe because Node's replica map is keyed by ShardNumber — at most one
            // replica per shard per node. (TAPIR membership is a set of distinct addresses;
            // two replicas of the same shard on one node is an invalid configuration.)
            if let Some(ref dir) = self.discovery_dir {
                dir.remove_own_shard(shard);
            }
            tracing::info!(?shard, "replica removed");
            true
        } else {
            false
        }
    }
}
