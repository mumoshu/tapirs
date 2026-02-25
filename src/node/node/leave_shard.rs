use super::*;

impl Node {
    /// Coordinate removing this node's replica from a shard via the shard-manager.
    ///
    /// Protocol flow:
    ///   1. POST /v1/leave { shard, listen_addr } to shard manager
    ///   2. Shard manager queries discovery, verifies addr is in shard
    ///   3. ShardManager::leave creates ShardClient with discovery membership
    ///   4. Broadcasts RemoveMember(addr) to all replicas
    ///   5. Replicas remove addr from membership, enter ViewChanging, view += 3
    ///   6. DoViewChange → leader collects f+1 addenda → StartView
    ///   7. Remaining replicas transition to Normal with clean membership
    ///   8. Removed replica is orphaned — safe to drop via remove_replica()
    ///
    /// After this returns, call `remove_replica(shard)` to drop the local handle.
    pub async fn leave_shard(&self, shard: ShardNumber) -> Result<(), String> {
        let listen_addr = {
            let replicas = self.replicas.lock().unwrap();
            let handle = replicas
                .get(&shard)
                .ok_or_else(|| format!("shard {shard:?} not found on this node"))?;
            handle.listen_addr
        };
        self.shard_manager_leave(shard, listen_addr).await
    }
}
