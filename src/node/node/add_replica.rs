use super::*;
use crate::node::types::ReplicaConfig;
use crate::{IrMembership, MvccDiskStore, TapirReplica, TcpTransport};
use std::time::Duration;

impl Node {
    pub async fn add_replica_no_join(&self, cfg: &ReplicaConfig) -> Result<(), String> {
        self.add_replica_inner(cfg, None).await
    }

    /// Add a replica using a pre-bound TCP listener (no TOCTOU port race).
    pub async fn add_replica_with_listener(
        &self,
        cfg: &ReplicaConfig,
        listener: std::net::TcpListener,
    ) -> Result<(), String> {
        self.add_replica_inner(cfg, Some(listener)).await
    }

    pub(crate) async fn add_replica_inner(
        &self,
        cfg: &ReplicaConfig,
        pre_bound_listener: Option<std::net::TcpListener>,
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

        // Populate shard directory so TapirTransport::shard_addresses works.
        transport.set_shard_addresses(shard, membership.clone());

        // Start listener BEFORE creating replica (IrReplica::new starts tick tasks).
        if let Some(listener) = pre_bound_listener {
            transport
                .listen_from_std(listener)
                .map_err(|e| format!("failed to listen on {listen_addr}: {e}"))?;
        } else {
            transport
                .listen(listen_addr)
                .await
                .map_err(|e| format!("failed to listen on {listen_addr}: {e}"))?;
        }

        let transport_for_replica = transport.clone();
        let mvcc_dir = format!("{}/shard_{}/mvcc", self.persist_dir, cfg.shard);
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        let backend = MvccDiskStore::open_with_flags(
            std::path::PathBuf::from(&mvcc_dir),
            crate::DiskOpenFlags { create: true, direct: true },
        )
        .map_err(|e| format!("failed to open DiskStore at {mvcc_dir}: {e}"))?;

        #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
        let backend = MvccDiskStore::open(
            std::path::PathBuf::from(&mvcc_dir),
        )
        .map_err(|e| format!("failed to open DiskStore at {mvcc_dir}: {e}"))?;
        let replica = Arc::new_cyclic(|weak: &std::sync::Weak<TapirIrReplica>| {
            let weak = weak.clone();
            transport_for_replica.set_receive_callback(move |from, message| {
                weak.upgrade()?.receive(from, message)
            });
            let upcalls = TapirReplica::new_with_backend(shard, false, backend);
            crate::IrReplica::with_view_change_interval(
                (self.new_rng)(),
                membership,
                upcalls,
                transport_for_replica.clone(),
                Some(TapirReplica::tick),
                Some(Duration::from_secs(10)),
                Default::default(),
            )
        });

        tracing::info!(?shard, %listen_addr, "replica started");

        self.replicas.lock().unwrap().insert(
            shard,
            ReplicaHandle {
                replica,
                listen_addr,
            },
        );

        // Register as own shard so CachingShardDirectory pushes membership for it.
        if let Some(ref dir) = self.discovery_dir {
            dir.add_own_shard(shard);
        }

        Ok(())
    }
}
