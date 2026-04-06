use super::*;
use crate::node::types::ReplicaConfig;
use crate::remote_store::config::S3StorageConfig;
use crate::{IrMembership, TcpTransport};
use std::time::Duration;

impl Node {
    /// Create a writable replica pre-populated from S3 via zero-copy clone.
    ///
    /// Same as add_replica_inner but calls open_production_stores_from_s3()
    /// instead of open_production_stores(). The clone downloads the manifest
    /// and registers S3CachingIo for lazy segment downloads. After bootstrap,
    /// the replica is fully independent and participates in consensus normally.
    pub async fn add_writable_clone_from_s3(
        &self,
        cfg: &ReplicaConfig,
        source_s3: S3StorageConfig,
    ) -> Result<(), String> {
        let shard = ShardNumber(cfg.shard);
        let listen_addr: SocketAddr = cfg
            .listen_addr
            .parse()
            .map_err(|e| format!("invalid listen_addr '{}': {e}", cfg.listen_addr))?;

        let membership_addrs: Vec<TcpAddress> = cfg
            .membership
            .iter()
            .map(|a| {
                a.parse()
                    .map(TcpAddress)
                    .map_err(|e| format!("invalid membership addr '{a}': {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let membership = IrMembership::new(membership_addrs);

        let address = TcpAddress(listen_addr);

        #[cfg(feature = "tls")]
        let transport = if let Some(ref tls_config) = self.tls_config {
            TcpTransport::with_tls(address, Arc::clone(&self.directory), tls_config)
                .map_err(|e| format!("TLS config error: {e}"))?
        } else {
            TcpTransport::with_directory(address, Arc::clone(&self.directory))
        };

        #[cfg(not(feature = "tls"))]
        let transport =
            TcpTransport::with_directory(address, Arc::clone(&self.directory));

        transport.set_shard_addresses(shard, membership.clone());

        transport
            .listen(listen_addr)
            .await
            .map_err(|e| format!("failed to listen on {listen_addr}: {e}"))?;

        // *** THE ONE DIFFERENCE: open from S3 instead of fresh ***
        // block_in_place is needed because open_production_stores_from_s3 uses
        // Handle::current().block_on() internally for async S3 calls.
        let transport_for_replica = transport.clone();
        let persist_dir = self.persist_dir.clone();
        let dest_s3 = self.s3_config.clone();
        let shard_id = cfg.shard;
        let (upcalls, ir_store) = tokio::task::block_in_place(|| {
            crate::store_defaults::open_production_stores_from_s3(
                shard,
                &persist_dir,
                shard_id,
                true,
                &source_s3,
                dest_s3,
            )
        })?;
        let replica = Arc::new_cyclic(|weak: &std::sync::Weak<S3BackedTapirIrReplica>| {
            let weak = weak.clone();
            transport_for_replica.set_receive_callback(move |from, message| {
                weak.upgrade()?.receive(from, message)
            });
            crate::IrReplica::with_view_change_interval(
                (self.new_rng)(),
                membership,
                upcalls,
                transport_for_replica.clone(),
                crate::store_defaults::s3_backed_app_tick(),
                Some(Duration::from_secs(10)),
                ir_store,
            )
        });

        tracing::info!(?shard, %listen_addr, "writable clone started");

        self.replicas.lock().unwrap().insert(
            shard,
            ReplicaHandle {
                replica: AnyReplica::S3Backed(replica),
                listen_addr,
            },
        );

        if let Some(ref dir) = self.discovery_dir {
            dir.add_own_shard(shard);
        }

        Ok(())
    }
}
