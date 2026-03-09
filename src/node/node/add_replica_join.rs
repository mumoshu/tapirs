use super::*;
use crate::node::types::ReplicaConfig;

impl Node {
    /// Add a replica and coordinate bootstrap/join via the shard-manager.
    ///
    /// 1. Add a local replica with membership=[self] via [`add_replica`](Self::add_replica).
    /// 2. POST /v1/join to the shard-manager, which decides whether to
    ///    bootstrap (first replica) or join (subsequent).
    pub async fn add_replica_join(
        &self,
        shard: ShardNumber,
        listen_addr: SocketAddr,
    ) -> Result<(), String> {
        let cfg = ReplicaConfig {
            shard: shard.0,
            listen_addr: listen_addr.to_string(),
            membership: vec![listen_addr.to_string()],
        };
        self.add_replica_no_join(&cfg).await?;

        // Shard-manager handles both bootstrap (first replica) and join (subsequent).
        self.shard_manager_join(shard, listen_addr).await?;
        Ok(())
    }
}
