use crate::discovery::backend::DiscoveryBackend;
use crate::discovery::{CachingShardDirectory, InMemoryShardDirectory};
use crate::node::types::{ReplicaConfig, ShardBackup};
use crate::{
    IrClient, IrMembership, IrReplica, IrReplicaMetrics, IrSharedView, IrView,
    IrViewNumber, MvccDiskStore, ShardNumber, TapirReplica, TcpAddress, TcpTransport,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub type TapirIrReplica = IrReplica<TapirReplica<String, String>, TcpTransport<TapirReplica<String, String>>>;

pub struct ReplicaHandle {
    pub replica: Arc<TapirIrReplica>,
    pub listen_addr: SocketAddr,
}

pub struct Node {
    pub replicas: Mutex<HashMap<ShardNumber, ReplicaHandle>>,
    pub(crate) persist_dir: String,
    pub(crate) directory: Arc<InMemoryShardDirectory<TcpAddress>>,
    /// Holds the CachingShardDirectory alive so its background sync task
    /// continues running. When None, no discovery sync is active.
    /// Also used to register/unregister own_shards for PUSH filtering.
    pub(crate) discovery_dir: Option<Arc<CachingShardDirectory<TcpAddress, String, DiscoveryBackend>>>,
    pub shard_manager_url: Option<String>,
    /// Factory function for creating new RNG instances.
    /// Kept out of library to avoid thread_rng() in library code.
    pub new_rng: fn() -> crate::Rng,
    #[cfg(feature = "tls")]
    pub tls_config: Option<crate::tls::TlsConfig>,
}

impl Node {
    pub fn new(persist_dir: String, new_rng: fn() -> crate::Rng) -> Self {
        Self {
            replicas: Mutex::new(HashMap::new()),
            persist_dir,
            directory: Arc::new(InMemoryShardDirectory::new()),
            discovery_dir: None,
            shard_manager_url: None,
            new_rng,
            #[cfg(feature = "tls")]
            tls_config: None,
        }
    }

    pub fn with_discovery_backend(
        persist_dir: String,
        backend: DiscoveryBackend,
        new_rng: fn() -> crate::Rng,
    ) -> Self {
        let directory = Arc::new(InMemoryShardDirectory::new());
        let discovery_dir = CachingShardDirectory::new(
            Arc::clone(&directory),
            Arc::new(backend),
            std::time::Duration::from_secs(10),
        );
        Self {
            replicas: Mutex::new(HashMap::new()),
            persist_dir,
            directory,
            discovery_dir: Some(discovery_dir),
            shard_manager_url: None,
            new_rng,
            #[cfg(feature = "tls")]
            tls_config: None,
        }
    }

    #[allow(dead_code)] // Used by integration tests
    pub fn with_discovery_backend_and_shard_manager(
        persist_dir: String,
        backend: DiscoveryBackend,
        shard_manager_url: &str,
        new_rng: fn() -> crate::Rng,
    ) -> Self {
        let mut node = Self::with_discovery_backend(persist_dir, backend, new_rng);
        node.shard_manager_url = Some(shard_manager_url.to_string());
        node
    }

    pub async fn add_replica_no_join(&self, cfg: &ReplicaConfig) -> Result<(), String> {
        self.add_replica_inner(cfg, None).await
    }

    /// Add a replica using a pre-bound TCP listener (no TOCTOU port race).
    #[allow(dead_code)] // Used by integration tests
    pub async fn add_replica_with_listener(
        &self,
        cfg: &ReplicaConfig,
        listener: std::net::TcpListener,
    ) -> Result<(), String> {
        self.add_replica_inner(cfg, Some(listener)).await
    }

    async fn add_replica_inner(
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

        let persist_dir = format!("{}/shard_{}", self.persist_dir, cfg.shard);
        let address = TcpAddress(listen_addr);

        #[cfg(feature = "tls")]
        let transport = if let Some(ref tls_config) = self.tls_config {
            TcpTransport::with_tls(address, persist_dir, Arc::clone(&self.directory), tls_config)
                .map_err(|e| format!("TLS config error: {e}"))?
        } else {
            TcpTransport::with_directory(address, persist_dir, Arc::clone(&self.directory))
        };

        #[cfg(not(feature = "tls"))]
        let transport =
            TcpTransport::with_directory(address, persist_dir, Arc::clone(&self.directory));

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
            IrReplica::with_view_change_interval(
                (self.new_rng)(),
                membership,
                upcalls,
                transport_for_replica.clone(),
                Some(TapirReplica::tick),
                Some(Duration::from_secs(10)),
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

    /// Add a replica and coordinate bootstrap/join via the shard-manager.
    ///
    /// 1. Add a local replica with membership=[self] via [`add_replica`](Self::add_replica).
    /// 2. POST /v1/join to the shard-manager, which decides whether to
    ///    bootstrap (first replica) or join (subsequent).
    pub async fn add_replica_join(
        &self,
        shard: ShardNumber,
        listen_addr: SocketAddr,
        storage: &str,
    ) -> Result<(), String> {
        if storage == "disk" {
            return Err(
                "disk storage backend is not yet available; use --storage memory (default)"
                    .to_string(),
            );
        }

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

    /// Send an HTTP POST to the shard-manager, handling both plain TCP and TLS.
    async fn shard_manager_http_post(
        &self,
        path: &str,
        shard: ShardNumber,
        listen_addr: SocketAddr,
    ) -> Result<(), String> {
        let url = self
            .shard_manager_url
            .as_ref()
            .ok_or_else(|| "no shard-manager-url configured".to_string())?;

        let (host_port, _is_https) = if let Some(hp) = url.strip_prefix("https://") {
            (hp, true)
        } else if let Some(hp) = url.strip_prefix("http://") {
            (hp, false)
        } else {
            (url.as_str(), false)
        };

        let body = serde_json::to_string(&serde_json::json!({
            "shard": shard.0,
            "listen_addr": listen_addr.to_string(),
        }))
        .map_err(|e| format!("serialize {path} request: {e}"))?;

        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {host_port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len(),
        );

        // Use string-based connect to support both IP:port and hostname:port.
        let tcp_stream = tokio::net::TcpStream::connect(host_port)
            .await
            .map_err(|e| format!("connect to shard-manager at {host_port}: {e}"))?;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        #[cfg(feature = "tls")]
        let response = if _is_https {
            let tls_config = self.tls_config.as_ref()
                .ok_or_else(|| "https:// shard-manager URL requires TLS config (--tls-cert/--tls-key/--tls-ca)".to_string())?;
            let connector = crate::tls::ReloadableTlsConnector::new(tls_config)
                .map_err(|e| format!("TLS connector: {e}"))?;
            let host = host_port.rsplit_once(':').map(|(h, _)| h).unwrap_or(host_port);
            let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
                .map_err(|e| format!("invalid TLS server name '{host}': {e}"))?;
            let mut tls_stream = connector.connector()
                .connect(server_name, tcp_stream).await
                .map_err(|e| format!("TLS handshake with shard-manager: {e}"))?;
            tls_stream.write_all(request.as_bytes()).await
                .map_err(|e| format!("send {path}: {e}"))?;
            let mut resp = Vec::new();
            tls_stream.read_to_end(&mut resp).await
                .map_err(|e| format!("read {path}: {e}"))?;
            resp
        } else {
            let mut stream = tcp_stream;
            stream.write_all(request.as_bytes()).await
                .map_err(|e| format!("send {path}: {e}"))?;
            let mut resp = Vec::new();
            stream.read_to_end(&mut resp).await
                .map_err(|e| format!("read {path}: {e}"))?;
            resp
        };

        #[cfg(not(feature = "tls"))]
        let response = {
            let _ = _is_https;
            let mut stream = tcp_stream;
            stream.write_all(request.as_bytes()).await
                .map_err(|e| format!("send {path}: {e}"))?;
            let mut resp = Vec::new();
            stream.read_to_end(&mut resp).await
                .map_err(|e| format!("read {path}: {e}"))?;
            resp
        };

        let resp_str = String::from_utf8_lossy(&response);
        let resp_body = resp_str
            .split_once("\r\n\r\n")
            .map(|(_, b)| b)
            .unwrap_or("");
        let status_ok = resp_str
            .lines()
            .next()
            .map(|line| line.contains("200"))
            .unwrap_or(false);

        if !status_ok {
            #[derive(serde::Deserialize)]
            struct ErrResp {
                error: String,
            }
            if let Ok(err) = serde_json::from_str::<ErrResp>(resp_body) {
                return Err(err.error);
            }
            return Err(format!("shard-manager error on {path}: {resp_body}"));
        }

        Ok(())
    }

    async fn shard_manager_join(
        &self,
        shard: ShardNumber,
        listen_addr: SocketAddr,
    ) -> Result<(), String> {
        self.shard_manager_http_post("/v1/join", shard, listen_addr).await
    }

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

    async fn shard_manager_leave(
        &self,
        shard: ShardNumber,
        listen_addr: SocketAddr,
    ) -> Result<(), String> {
        self.shard_manager_http_post("/v1/leave", shard, listen_addr).await
    }

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

    pub fn force_view_change(&self, shard: ShardNumber) -> bool {
        let replicas = self.replicas.lock().unwrap();
        if let Some(handle) = replicas.get(&shard) {
            handle.replica.force_view_change();
            tracing::info!(?shard, "view change triggered");
            true
        } else {
            false
        }
    }

    /// Returns the current view number of the replica hosting `shard` on this node,
    /// or None if this node has no replica for that shard.
    #[allow(dead_code)] // Used by integration tests
    pub fn shard_view_number(&self, shard: ShardNumber) -> Option<u64> {
        let replicas = self.replicas.lock().unwrap();
        replicas.get(&shard).map(|h| h.replica.view_number())
    }

    #[allow(clippy::disallowed_methods)] // output order unspecified; used for display only
    pub fn shard_list(&self) -> Vec<(ShardNumber, SocketAddr)> {
        self.replicas
            .lock()
            .unwrap()
            .iter()
            .map(|(shard, handle)| (*shard, handle.listen_addr))
            .collect()
    }

    /// Collect metrics from all replicas on this node for Prometheus exposition.
    #[allow(clippy::disallowed_methods)] // iteration order unimportant for metrics
    pub fn collect_metrics(&self) -> Vec<(ShardNumber, IrReplicaMetrics)> {
        self.replicas
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(shard, handle)| {
                handle.replica.collect_metrics().map(|m| (*shard, m))
            })
            .collect()
    }

    /// Take a backup of a shard by fetching the leader_record from
    /// the local replica.
    ///
    /// Returns the IR record and view from the last completed view change.
    /// Force a view change before backup if the most up-to-date state is
    /// needed — view change is TAPIR's synchronization mechanism.
    pub async fn backup_shard(&self, shard: ShardNumber) -> Option<ShardBackup> {
        let (transport, addr) = {
            let replicas = self.replicas.lock().unwrap();
            let handle = replicas.get(&shard)?;
            (handle.replica.transport().clone(), handle.listen_addr)
        };
        let client = IrClient::new(
            (self.new_rng)(),
            IrMembership::new(vec![TcpAddress(addr)]),
            transport,
        );
        let (view, record) = client.fetch_leader_record().await?;
        Some(ShardBackup {
            record: (*record).clone(),
            view,
        })
    }

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
