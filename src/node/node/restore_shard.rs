use super::*;
use crate::node::types::{ReplicaConfig, ShardBackup};
use crate::{IrClient, IrMembership, IrSharedView, IrView, IrViewNumber};

impl Node {
    /// Restore a shard from backup onto this node.
    ///
    /// Creates a fresh replica at listen_addr, then sends a BootstrapRecord
    /// with the backup data via IrClient::bootstrap_record(). The replica
    /// converts it to a self-directed StartView, and TAPIR's sync() replays
    /// all operations from the record to reconstruct the OCC+MVCC state.
    ///
    /// For multi-replica restore: call on each node with the same
    /// new_membership list, then register with discovery.
    pub async fn restore_shard(
        &self,
        shard: ShardNumber,
        listen_addr: SocketAddr,
        backup: &ShardBackup,
        new_membership: Vec<SocketAddr>,
    ) -> Result<(), String> {
        self.restore_shard_inner(shard, listen_addr, backup, new_membership, None)
            .await
    }

    /// Restore a shard using a pre-bound TCP listener (no TOCTOU port race).
    #[allow(dead_code)] // Used by integration tests
    pub async fn restore_shard_with_listener(
        &self,
        shard: ShardNumber,
        listen_addr: SocketAddr,
        backup: &ShardBackup,
        new_membership: Vec<SocketAddr>,
        listener: std::net::TcpListener,
    ) -> Result<(), String> {
        self.restore_shard_inner(shard, listen_addr, backup, new_membership, Some(listener))
            .await
    }

    async fn restore_shard_inner(
        &self,
        shard: ShardNumber,
        listen_addr: SocketAddr,
        backup: &ShardBackup,
        new_membership: Vec<SocketAddr>,
        pre_bound_listener: Option<std::net::TcpListener>,
    ) -> Result<(), String> {
        // Create fresh replica with membership=[self].
        let cfg = ReplicaConfig {
            shard: shard.0,
            listen_addr: listen_addr.to_string(),
            membership: vec![listen_addr.to_string()],
        };
        self.add_replica_inner(&cfg, pre_bound_listener).await?;

        // Build restore view: new membership, advanced view number,
        // preserved app_config (shard key ranges).
        let restore_view = IrSharedView::new(IrView {
            membership: IrMembership::new(
                new_membership.into_iter().map(TcpAddress).collect(),
            ),
            number: IrViewNumber(backup.view.number.0 + 10),
            app_config: backup.view.app_config.clone(),
        });

        // Clone the replica's transport and create a temporary IrClient
        // to send BootstrapRecord — same pattern as join()
        // in sharding/shardmanager/catchup.rs.
        let transport = {
            let replicas = self.replicas.lock().unwrap();
            let handle = replicas.get(&shard)
                .ok_or_else(|| format!("shard {shard:?} not found after creation"))?;
            handle.replica.transport().clone()
        };
        let client = IrClient::new(
            (self.new_rng)(),
            IrMembership::new(vec![TcpAddress(listen_addr)]),
            transport,
        );
        client.bootstrap_record(backup.record.clone(), restore_view);

        tracing::info!(?shard, %listen_addr, "shard restored from backup");
        Ok(())
    }
}
