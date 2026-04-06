use super::*;
use crate::backup::s3backup::S3BackupStorage;
use crate::backup::storage::BackupStorage;
use crate::node::read_replica_shim::ReadReplicaShim;
use crate::remote_store::config::S3StorageConfig;
use crate::remote_store::manifest_store::RemoteManifestStore;
use crate::remote_store::read_replica::ReadReplica;
use crate::remote_store::read_replica_refresh::spawn_refresh_task;
use crate::{IrMembership, TcpTransport};
use std::time::Duration;

impl Node {
    /// Create a read-only replica backed by S3 with auto-refresh.
    ///
    /// Opens a `ReadReplica` from S3, spawns a background refresh task,
    /// creates a `TcpTransport` with a `ReadReplicaShim` as the receive
    /// callback, and stores the handle in `read_replicas`.
    ///
    /// The shim handles `RequestUnlogged` messages only (GetAt, ScanAt,
    /// Get, Scan). All other IR messages are dropped — no consensus.
    pub async fn add_read_replica_from_s3(
        &self,
        shard: ShardNumber,
        listen_addr: SocketAddr,
        source_s3: S3StorageConfig,
        refresh_interval: Duration,
    ) -> Result<(), String> {
        let base_dir = format!("{}/read_replica_shard_{}", self.persist_dir, shard.0);
        std::fs::create_dir_all(&base_dir)
            .map_err(|e| format!("create dir {base_dir}: {e}"))?;

        // Open ReadReplica from S3.
        let storage = S3BackupStorage::new(
            &source_s3.bucket,
            &source_s3.prefix,
            source_s3.region.as_deref(),
            source_s3.endpoint_url.as_deref(),
        )
        .await;
        let man_store = Arc::new(RemoteManifestStore::new(storage.sub("")));
        let replica = Arc::new(
            ReadReplica::open(
                &*man_store,
                source_s3,
                shard,
                std::path::Path::new(&base_dir),
            )
            .await
            .map_err(|e| format!("ReadReplica::open: {e}"))?,
        );

        // Spawn background refresh task.
        let refresh_task = spawn_refresh_task(&replica, man_store, refresh_interval);

        // Transport is parameterized on ProductionTapirReplica for wire
        // compatibility — the IR message types (UO, UR, etc.) are determined
        // by the app type parameter. Clients send messages typed on
        // ProductionTapirReplica, so the shim's transport must match.
        let address = TcpAddress(listen_addr);

        #[cfg(feature = "tls")]
        let transport =
            if let Some(ref tls_config) = self.tls_config {
                TcpTransport::<crate::store_defaults::ProductionTapirReplica>::with_tls(
                    address,
                    Arc::clone(&self.directory),
                    tls_config,
                )
                .map_err(|e| format!("TLS: {e}"))?
            } else {
                TcpTransport::<crate::store_defaults::ProductionTapirReplica>::with_directory(
                    address,
                    Arc::clone(&self.directory),
                )
            };

        #[cfg(not(feature = "tls"))]
        let transport =
            TcpTransport::<crate::store_defaults::ProductionTapirReplica>::with_directory(
                address,
                Arc::clone(&self.directory),
            );

        // Register shim as receive callback.
        let shim = ReadReplicaShim::new(Arc::clone(&replica), address);
        transport.set_receive_callback(move |from, message| shim.receive(from, message));

        // Set shard addresses so the transport knows this shard exists.
        transport.set_shard_addresses(shard, IrMembership::new(vec![address]));
        transport
            .listen(listen_addr)
            .await
            .map_err(|e| format!("listen on {listen_addr}: {e}"))?;

        tracing::info!(?shard, %listen_addr, "read replica started");
        self.read_replicas.lock().unwrap().insert(
            shard,
            ReadReplicaHandle {
                listen_addr,
                _refresh_task: refresh_task,
            },
        );

        if let Some(ref dir) = self.discovery_dir {
            dir.add_own_shard(shard);
        }

        Ok(())
    }
}
